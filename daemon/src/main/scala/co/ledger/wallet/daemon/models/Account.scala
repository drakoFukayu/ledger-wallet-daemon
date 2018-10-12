package co.ledger.wallet.daemon.models

import co.ledger.core
import co.ledger.core.{BitcoinLikePickingStrategy, OperationOrderKey}
import co.ledger.core.implicits._
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

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object Account {

  class Account(private val coreA: core.Account, private val wallet: Wallet) extends Logging {
    private[this] val self = this
    private val _coreExecutionContext = LedgerCoreExecutionContext.newThreadPool()
    implicit val ec: ExecutionContext = MDCPropagatingExecutionContext.Implicits.global

    val index: Int = coreA.getIndex

    private val isBitcoin: Boolean = coreA.isInstanceOfBitcoinLikeAccount

    def balance: Future[Long] = coreA.getBalance().map { balance =>
      debug(s"Account $index: ${balance.toLong}")
      balance.toLong
    }

    def accountView: Future[AccountView] = balance.map { b =>
      AccountView(wallet.name, index, b, coreA.getRestoreKey, wallet.currency.currencyView)
    }

    def signTransaction(rawTx: Array[Byte], signatures: Seq[(Array[Byte], Array[Byte])]): Future[String] = {
      val tx = wallet.currency.parseUnsignedTransaction(rawTx)

      if (tx.getInputs.size != signatures.size) throw new scala.IllegalArgumentException("Signatures and transaction inputs size not matching")
      else if (isBitcoin) {
        tx.getInputs.asScala.zipWithIndex.foreach { case (input, index) =>
          input.pushToScriptSig(signatures(index)._1) // signature
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