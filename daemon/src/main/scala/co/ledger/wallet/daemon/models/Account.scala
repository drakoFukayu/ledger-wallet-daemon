package co.ledger.wallet.daemon.models

import java.util.{Calendar, Date}

import co.ledger.core
import co.ledger.core.implicits._
import co.ledger.core.{BitcoinLikePickingStrategy, OperationOrderKey}
import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext
import co.ledger.wallet.daemon.clients.ClientFactory
import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.controllers.TransactionsController.TransactionInfo
import co.ledger.wallet.daemon.libledger_core.async.LedgerCoreExecutionContext
import co.ledger.wallet.daemon.models.coins.Bitcoin
import co.ledger.wallet.daemon.models.coins.Coin.TransactionView
import co.ledger.wallet.daemon.schedulers.observers.{SynchronizationEventReceiver, SynchronizationResult}
import co.ledger.wallet.daemon.services.LogMsgMaker
import co.ledger.wallet.daemon.utils.HexUtils
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.primitives.UnsignedInteger
import com.twitter.inject.Logging

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

object Account {

  class Account(private val coreA: core.Account, private val wallet: Wallet) extends Logging {
    private[this] val self = this
    private val _coreExecutionContext = LedgerCoreExecutionContext.newThreadPool()
    implicit val ec: ExecutionContext = MDCPropagatingExecutionContext.Implicits.global

    val index: Int = coreA.getIndex

    private val isBitcoin: Boolean = coreA.isInstanceOfBitcoinLikeAccount

    def balance: Future[Long] = coreA.getBalance().map { balance =>
      debug(s"Account $index, balance: ${balance.toLong}")
      balance.toLong
    }

    def accountView: Future[AccountView] = {
      for {
        ba <- balance
        ops <- operationCounts
      } yield AccountView(wallet.name, index, ba, ops, coreA.getRestoreKey, wallet.currency.currencyView)
    }

    def signTransaction(rawTx: Array[Byte], signatures: Seq[(Array[Byte], Array[Byte])]): Future[String] = {
      val tx = wallet.currency.parseUnsignedTransaction(rawTx)

      if (tx.getInputs.size != signatures.size) throw new scala.IllegalArgumentException("Signatures and transaction inputs size not matching")
      else if (isBitcoin) {
        tx.getInputs.asScala.zipWithIndex.foreach { case (input, index) =>
          input.pushToScriptSig(wallet.currency.concateSig(signatures(index)._1)) // signature
          input.pushToScriptSig(signatures(index)._2) // pubkey
        }
        debug(s"transaction after sign '${HexUtils.valueOf(tx.serialize())}'")
        coreA.asBitcoinLikeAccount().broadcastTransaction(tx)
      } else throw new UnsupportedOperationException("Account type not supported, can't sign transaction")
    }

    def createTransaction(transactionInfo: TransactionInfo): Future[TransactionView] = {

        if (isBitcoin) {
          val feesPerByte: Future[core.Amount] = transactionInfo.feeAmount map { amount =>
             Future.successful(wallet.currency.convertAmount(amount))
            } getOrElse {
            ClientFactory.apiClient.getFees(wallet.currency.name).map { feesInfo =>
              wallet.currency.convertAmount(feesInfo.getAmount(transactionInfo.feeMethod.get))
            }
          }
          feesPerByte.flatMap { fees =>
            val tx = coreA.asBitcoinLikeAccount().buildTransaction()
              .sendToAddress(wallet.currency.convertAmount(transactionInfo.amount), transactionInfo.recipient)
              .pickInputs(BitcoinLikePickingStrategy.DEEP_OUTPUTS_FIRST, UnsignedInteger.MAX_VALUE.intValue)
              .setFeesPerByte(fees)
            transactionInfo.excludeUtxos.foreach { case (previousTx, outputIndex) =>
                tx.excludeUtxo(previousTx, outputIndex)
            }
            tx.build().flatMap { t =>
              Bitcoin.newUnsignedTransactionView(t, fees.toLong)
            }
          }
        } else throw new UnsupportedOperationException("Account type not supported, can't create transaction")
    }

    def operation(uid: String, fullOp: Int): Future[Option[Operation]] = {
      val queryOperations = coreA.queryOperations()
      queryOperations.filter().opAnd(core.QueryFilter.operationUidEq(uid))
      (if (fullOp > 0) {
        queryOperations.complete().execute()
      } else { queryOperations.partial().execute() }).map { operations =>
        operations.asScala.headOption.map { o => Operation.newInstance(o, self, wallet)}
      }
    }

    def operations(offset: Long, batch: Int, fullOp: Int): Future[Seq[Operation]] = {
      (if (fullOp > 0) {
        coreA.queryOperations().addOrder(OperationOrderKey.DATE, true).offset(offset).limit(batch).complete().execute()
      } else {
        coreA.queryOperations().addOrder(OperationOrderKey.DATE, true).offset(offset).limit(batch).partial().execute()
      }).map { operations =>
        if (operations.size() <= 0) { List[Operation]() }
        else { operations.asScala.map { o => Operation.newInstance(o, self, wallet)} }
      }
    }

    def operationCounts: Future[Map[core.OperationType, Int]] =
      coreA.queryOperations().addOrder(OperationOrderKey.DATE, true).partial().execute().map { operations =>
        operations.asScala.toList.groupBy(op => op.getOperationType).map { case (optType, opts) => (optType, opts.size)}
      }

    def balances(start: String, end: String, timePeriod: core.TimePeriod): Future[List[Long]] = {
      coreA.getBalanceHistory(start, end, timePeriod).map { balances =>
        balances.asScala.toList.map { ba => ba.toLong }
      }
    }

    // TODO: refactor this part once lib-core provides the feature
    def operationsCounts(start: Date, end: Date, timePeriod: core.TimePeriod): Future[List[Map[core.OperationType, Int]]] = {
      coreA.queryOperations().addOrder(OperationOrderKey.DATE, true).partial().execute().map { operations =>
        val ops = operations.asScala.toList.filter( op => op.getDate.compareTo(start) >= 0 && op.getDate.compareTo(end) <= 0)
        filter(start, 1, end, standardTimePeriod(timePeriod), ops, Nil)
      }
    }

    @tailrec
    private def filter(start: Date, i: Int, end: Date, timePeriod: Int, operations: List[core.Operation], preResult: List[Map[core.OperationType, Int]]): List[Map[core.OperationType, Int]] = {
      def searchResult(condition: core.Operation => Boolean): Map[core.OperationType, Int] =
        operations.filter(condition).groupBy(op => op.getOperationType).map { case(optType, opts) => (optType, opts.size) }

      val (begin, next) = {
        val calendar = Calendar.getInstance()
        calendar.setTime(start)
        calendar.add(timePeriod, i - 1)
        val begin = calendar.getTime
        calendar.add(timePeriod, 1)
        (begin, calendar.getTime)
      }
      if (end.after(next)) {
        val result = searchResult(op => op.getDate.compareTo(begin) >= 0 && op.getDate.compareTo(next) < 0)
        filter(start, i + 1, end, timePeriod, operations, preResult:::List(result))
      } else {
        val result = searchResult(op => op.getDate.compareTo(begin) >= 0 && op.getDate.compareTo(end) <= 0)
        preResult ::: List(result)
      }
    }

    private def standardTimePeriod(timePeriod: core.TimePeriod): Int = timePeriod match {
      case core.TimePeriod.DAY => Calendar.DATE
      case core.TimePeriod.MONTH => Calendar.MONTH
      case core.TimePeriod.WEEK => Calendar.WEEK_OF_MONTH
    }

    def freshAddresses(): Future[Seq[String]] = {
      coreA.getFreshPublicAddresses().map(_.asScala.map(_.toString))
    }

    def sync(poolName: String): Future[SynchronizationResult] = {
      val promise: Promise[SynchronizationResult] = Promise[SynchronizationResult]()
      val receiver: core.EventReceiver = new SynchronizationEventReceiver(coreA.getIndex, wallet.name, poolName, promise)
      coreA.synchronize().subscribe(_coreExecutionContext, receiver)
      debug(s"Synchronize $self")
      promise.future
    }

    def startRealTimeObserver(): Unit = {
      if (DaemonConfiguration.realTimeObserverOn && !coreA.isObservingBlockchain) coreA.startBlockchainObservation()
      debug(LogMsgMaker.newInstance(s"Set real time observer on ${coreA.isObservingBlockchain}").append("account", self).toString())
    }

    def stopRealTimeObserver(): Unit = {
      debug(LogMsgMaker.newInstance("Stop real time observer").append("account", self).toString())
      if (coreA.isObservingBlockchain) coreA.stopBlockchainObservation()
    }

    override def toString: String = s"Account(index: $index)"
  }

  class Derivation(private val accountCreationInfo: core.AccountCreationInfo) {
    val index: Int = accountCreationInfo.getIndex

    lazy val view: AccountDerivationView = {
      val paths = accountCreationInfo.getDerivations.asScala
      val owners = accountCreationInfo.getOwners.asScala
      val pubKeys = {
        val pks = accountCreationInfo.getPublicKeys
        if (pks.isEmpty) { paths.map { _ => "" } }
        else { pks.asScala.map(HexUtils.valueOf) }
      }
      val chainCodes = {
        val ccs = accountCreationInfo.getChainCodes
        if (ccs.isEmpty) { paths.map { _ => "" } }
        else { ccs.asScala.map(HexUtils.valueOf) }
      }
      val derivations = paths.indices.map { i =>
        DerivationView(
          paths(i),
          owners(i),
          pubKeys(i) match {
            case "" => None
            case pubKey => Option(pubKey)
          },
          chainCodes(i) match {
            case "" => None
            case chainCode => Option(chainCode)
          })
      }
      AccountDerivationView(index, derivations)
    }
  }

  class ExtendedDerivation(info: core.ExtendedKeyAccountCreationInfo) {
    val index: Int = info.getIndex
    lazy val view: AccountExtendedDerivationView = {
      val extKeys = info.getExtendedKeys.asScala.map(Option.apply).padTo(info.getDerivations.size(), None)
      val derivations = (info.getDerivations.asScala, info.getOwners.asScala, extKeys).zipped map {
        case (path, owner, key) =>
          ExtendedDerivationView(path, owner, key)
      }
      AccountExtendedDerivationView(index, derivations.toList)
    }
  }

  def newInstance(coreA: core.Account, wallet: Wallet): Account = {
    new Account(coreA, wallet)
  }

  def newDerivation(coreD: core.AccountCreationInfo): Derivation = {
    new Derivation(coreD)
  }
}

case class AccountView(
                        @JsonProperty("wallet_name") walletName: String,
                        @JsonProperty("index") index: Int,
                        @JsonProperty("balance") balance: Long,
                        @JsonProperty("operation_count") operationCounts: Map[core.OperationType, Int],
                        @JsonProperty("keychain") keychain: String,
                        @JsonProperty("currency") currency: CurrencyView
                      )

case class DerivationView(
                           @JsonProperty("path") path: String,
                           @JsonProperty("owner") owner: String,
                           @JsonProperty("pub_key") pubKey: Option[String],
                           @JsonProperty("chain_code") chainCode: Option[String]
                         ) {
  override def toString: String = s"DerivationView(path: $path, owner: $owner, pub_key: $pubKey, chain_code: $chainCode)"
}

case class AccountDerivationView(
                                  @JsonProperty("account_index") accountIndex: Int,
                                  @JsonProperty("derivations") derivations: Seq[DerivationView]
                                ) {

  override def toString: String = s"AccountDerivationView(account_index: $accountIndex, derivations: $derivations)"
}

case class ExtendedDerivationView(
                                   @JsonProperty("path") path: String,
                                   @JsonProperty("owner") owner: String,
                                   @JsonProperty("extended_key") extKey: Option[String],
                                 )

case class AccountExtendedDerivationView(
                                          @JsonProperty("account_index") accountIndex: Int,
                                          @JsonProperty("derivations") derivations: Seq[ExtendedDerivationView]
                                        )