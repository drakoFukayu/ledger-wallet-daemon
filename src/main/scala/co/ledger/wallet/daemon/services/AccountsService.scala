package co.ledger.wallet.daemon.services

import java.net.URL
import java.util.{Date, UUID}

import cats.instances.future._
import cats.instances.list._
import cats.instances.option._
import cats.syntax.either._
import cats.syntax.traverse._
import co.ledger.core
import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext.Implicits.global
import co.ledger.wallet.daemon.clients.{ClientFactory, ScalaHttpClientPool}
import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.database.DaemonCache
import co.ledger.wallet.daemon.exceptions.{ERC20NotFoundException, FallbackBalanceProviderException, ResyncOnGoingException}
import co.ledger.wallet.daemon.models.Account._
import co.ledger.wallet.daemon.models.Currency._
import co.ledger.wallet.daemon.models.Operations.{OperationView, PackedOperationsView}
import co.ledger.wallet.daemon.models.Wallet._
import co.ledger.wallet.daemon.models._
import co.ledger.wallet.daemon.schedulers.observers.SynchronizationResult
import co.ledger.wallet.daemon.utils.Utils
import co.ledger.wallet.daemon.utils.Utils.{RichBigInt, _}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import javax.inject.{Inject, Singleton}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@Singleton
class AccountsService @Inject()(daemonCache: DaemonCache, synchronizerManager: AccountSynchronizerManager, publisher: Publisher) extends DaemonService {

  case class CacheKey(a: AccountInfo, contract: Option[String])

  // Caching Future result of getBalance in order to share same Future to every requests
  private val balanceCache =
    CacheBuilder.newBuilder()
      .maximumSize(DaemonConfiguration.balanceCacheMaxSize)
      .expireAfterWrite(java.time.Duration.ofMinutes(DaemonConfiguration.balanceCacheTtlMin))
      .build[CacheKey, Future[BigInt]](new CacheLoader[CacheKey, Future[BigInt]] {
        def load(key: CacheKey): Future[BigInt] = {
          loadBalance(key.contract, key.a)
        }
      })

  def accounts(walletInfo: WalletInfo): Future[Seq[AccountView]] = {
    daemonCache.withWallet(walletInfo) { wallet =>
      wallet.accounts.flatMap { as =>
        as.toList.map { a =>
          val syncStatus = synchronizerManager.getSyncStatus(AccountInfo(a.getIndex, walletInfo)).get
          a.accountView(walletInfo.walletName, wallet.getCurrency.currencyView, syncStatus)
        }.sequence[Future, AccountView]
      }
    }
  }

  def account(accountInfo: AccountInfo): Future[Option[AccountView]] = {
    daemonCache.withWallet(accountInfo.walletInfo) { wallet =>
      wallet.account(accountInfo.accountIndex).flatMap(ao =>
        ao.map { account =>
          val syncStatus = synchronizerManager.getSyncStatus(accountInfo).get
          account.accountView(accountInfo.walletName, wallet.getCurrency.currencyView, syncStatus)
        }.sequence)
    }
  }

  /**
    * This method will wipe all the operations in the account, and resynchronize them from the
    * explorer. During the resynchronization, the account will not be accessible.
    */
  def resynchronizeAccount(accountInfo: AccountInfo): Unit = {
    synchronizerManager.resyncAccount(accountInfo)
  }

  def syncStatus(accountInfo: AccountInfo): Option[SyncStatus] = {
    synchronizerManager.getSyncStatus(accountInfo)
  }

  /**
    * Method to synchronize account operations from public resources. The method may take a while
    * to finish. This method only synchronize a single account.
    *
    * @return a Future of sequence of result of synchronization.
    */
  def synchronizeAccount(accountInfo: AccountInfo): Future[Seq[SynchronizationResult]] =
    daemonCache.withAccount(accountInfo)(_.sync(accountInfo.poolName, accountInfo.walletName).map(Seq(_)))

  def getAccount(accountInfo: AccountInfo): Future[Option[core.Account]] = daemonCache.getAccount(accountInfo: AccountInfo)

  def getBalance(contract: Option[String], accountInfo: AccountInfo): Future[BigInt] = {
    checkSyncStatus(accountInfo)
    balanceCache.get(CacheKey(accountInfo, contract))
  }

  def getUtxo(accountInfo: AccountInfo, offset: Int, batch: Int): Future[(List[UTXOView], Int)] = {
    checkSyncStatus(accountInfo)
    daemonCache.withAccountAndWallet(accountInfo) { (account, wallet) =>
      for {
        lastBlockHeight <- wallet.lastBlockHeight
        count <- account.getUtxoCount()
        utxos <- account.getUtxo(offset, batch).map(_.map(output => {
          val confirmations: Long =
            if (output.getBlockHeight >= 0) lastBlockHeight - output.getBlockHeight else output.getBlockHeight
          UTXOView(
            output.getTransactionHash,
            output.getOutputIndex,
            output.getAddress,
            output.getBlockHeight,
            confirmations,
            output.getValue.toBigInt.asScala)
        }))
      } yield (utxos, count)
    }
  }

  private def loadBalance(contract: Option[String], accountInfo: AccountInfo): Future[BigInt] = {
    checkSyncStatus(accountInfo)

    def runWithFallback(cb: Future[BigInt], url: URL, service: ScalaHttpClientPool, timeout: scala.concurrent.duration.Duration): Future[BigInt] = {
      Future(Await.result(cb, timeout)).recoverWith { case t =>
        warn(s"Using fallback provider $url due to Failed to get balance from libcore: $t")
        for {
          address <- accountFreshAddresses(accountInfo).map(_.head.address)
          balance <- FallbackService.getBalance(contract, address, service, url)
            .getOrElseF(Future.failed(FallbackBalanceProviderException(accountInfo.walletInfo.walletName, url.getHost, url.getPath)))
        } yield balance
      }
    }

    val balance = {
      daemonCache.withAccount(accountInfo)(a => {
        contract match {
          case Some(c) => a.erc20Balance(c).recoverWith({
            case _: ERC20NotFoundException => Future.successful(scala.BigInt(0))
          })
          case None => a.balance
        }
      })
    }

    daemonCache.withWallet(accountInfo.walletInfo) { wallet =>
      ClientFactory.apiClient.fallbackService(wallet.getCurrency.getName) match {
        case Some((url, service)) =>
          runWithFallback(balance, url, service, DaemonConfiguration.explorer.api.fallbackTimeout.milliseconds)
        case None =>
          balance
      }
    }
  }

  def getXpub(accountInfo: AccountInfo): Future[String] = {
    checkSyncStatus(accountInfo)
    daemonCache.withAccount(accountInfo) { a => Future.successful(a.getRestoreKey) }
  }

  def getERC20Operations(accountInfo: AccountInfo): Future[List[OperationView]] = {
    checkSyncStatus(accountInfo)
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        account.erc20Operations.flatMap { operations =>
          operations.traverse { case (coreOp, erc20Op) =>
            Operations.getErc20View(erc20Op, coreOp, wallet, account)
          }
        }
    }
  }

  def getBatchedERC20Operations(tokenAccountInfo: TokenAccountInfo, offset: Int, batch: Int): Future[List[OperationView]] = {
    checkSyncStatus(tokenAccountInfo.accountInfo)
    daemonCache.withAccountAndWallet(tokenAccountInfo.accountInfo) {
      case (account, wallet) =>
        account.batchedErc20Operations(tokenAccountInfo.tokenAddress, offset, batch).flatMap { operations =>
          operations.traverse { case (coreOp, erc20Op) =>
            Operations.getErc20View(erc20Op, coreOp, wallet, account)
          }
        }.recoverWith({
          case _: ERC20NotFoundException => Future.successful(List.empty)
        })
    }
  }

  def getBatchedERC20Operations(accountInfo: AccountInfo, offset: Int, batch: Int): Future[List[OperationView]] = {
    checkSyncStatus(accountInfo)
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        account.batchedErc20Operations(offset, batch).flatMap { operations =>
          operations.traverse { case (coreOp, erc20Op) =>
            Operations.getErc20View(erc20Op, coreOp, wallet, account)
          }
        }
    }
  }

  def getTokenAccounts(accountInfo: AccountInfo): Future[List[ERC20AccountView]] = {
    checkSyncStatus(accountInfo)
    daemonCache.withAccount(accountInfo) { account =>
      for {
        erc20Accounts <- account.erc20Accounts.liftTo[Future]
        erc20Balances <- account.erc20Balances(Some(erc20Accounts.map(acc => acc.getToken.getContractAddress).toArray))
        views <- erc20Accounts.zip(erc20Balances).map(v => ERC20AccountView.fromERC20Account(v._1, v._2)).sequence
      } yield views
    }
  }

  def getTokenAccount(tokenAccountInfo: TokenAccountInfo): Future[ERC20AccountView] = {
    checkSyncStatus(tokenAccountInfo.accountInfo)
    daemonCache.withAccount(tokenAccountInfo.accountInfo) { account =>
      for {
        erc20Account <- account.erc20Account(tokenAccountInfo.tokenAddress).liftTo[Future]
        balance <- account.erc20Balances(Some(Array[String](tokenAccountInfo.tokenAddress)))
        view <- ERC20AccountView.fromERC20Account(erc20Account, balance.head)
      } yield view
    }
  }

  def getTokenCoreAccount(tokenAccountInfo: TokenAccountInfo): Future[core.ERC20LikeAccount] = {
    checkSyncStatus(tokenAccountInfo.accountInfo)
    daemonCache.withAccount(tokenAccountInfo.accountInfo)(_.erc20Account(tokenAccountInfo.tokenAddress).liftTo[Future])
  }

  def getTokenCoreAccountBalanceHistory(tokenAccountInfo: TokenAccountInfo, startDate: Date, endDate: Date, period: core.TimePeriod): Future[List[BigInt]] = {
    checkSyncStatus(tokenAccountInfo.accountInfo)
    getTokenCoreAccount(tokenAccountInfo).map(_.getBalanceHistoryFor(startDate, endDate, period).asScala.map(_.asScala).toList)
      .recoverWith({
        case _: ERC20NotFoundException => Future.successful(List.fill(Utils.intervalSize(startDate, endDate, period))(BigInt(0)))
      })
  }

  def accountFreshAddresses(accountInfo: AccountInfo): Future[Seq[FreshAddressView]] = {
    checkSyncStatus(accountInfo)
    daemonCache.getFreshAddresses(accountInfo)
  }

  def accountAddressesInRange(from: Long, to: Long, accountInfo: AccountInfo): Future[Seq[FreshAddressView]] = {
    checkSyncStatus(accountInfo)
    daemonCache.getAddressesInRange(from, to, accountInfo)
  }

  def accountDerivationPath(accountInfo: AccountInfo): Future[String] =
    daemonCache.withWallet(accountInfo.walletInfo)(_.accountDerivationPathInfo(accountInfo.accountIndex))

  def nextAccountCreationInfo(accountIndex: Option[Int], walletInfo: WalletInfo): Future[AccountDerivationView] =
    daemonCache.withWallet(walletInfo)(_.accountCreationInfo(accountIndex)).map(_.view)

  def nextExtendedAccountCreationInfo(accountIndex: Option[Int], walletInfo: WalletInfo): Future[AccountExtendedDerivationView] =
    daemonCache.withWallet(walletInfo)(_.accountExtendedCreation(accountIndex)).map(_.view)

  def accountOperations(queryParams: OperationQueryParams, accountInfo: AccountInfo): Future[PackedOperationsView] = {
    checkSyncStatus(accountInfo)
    (queryParams.next, queryParams.previous) match {
      case (Some(n), _) =>
        // next has more priority, using database batch instead queryParams.batch
        info(LogMsgMaker.newInstance("Retrieve next batch operation").toString())
        daemonCache.getNextBatchAccountOperations(n, queryParams.fullOp, accountInfo)
      case (_, Some(p)) =>
        info(LogMsgMaker.newInstance("Retrieve previous operations").toString())
        daemonCache.getPreviousBatchAccountOperations(p, queryParams.fullOp, accountInfo)
      case _ =>
        // new request
        info(LogMsgMaker.newInstance("Retrieve latest operations").toString())
        daemonCache.getAccountOperations(queryParams.batch, queryParams.fullOp, accountInfo)
    }
  }

  def firstOperation(accountInfo: AccountInfo): Future[Option[OperationView]] = {
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        checkSyncStatus(accountInfo)
        account.firstOperation flatMap {
          case None => Future.successful(None)
          case Some(o) => Operations.getView(o, wallet, account).map(Some(_))
        }
    }
  }
  def latestOperations(accountInfo: AccountInfo, latests: Int): Future[Seq[OperationView]] = {
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        account.latestOperations(latests)
          .flatMap(ops => Future.sequence(
            ops.map(Operations.getView(_, wallet, account)))
        )
    }
  }

  def accountOperation(uid: String, fullOp: Int, accountInfo: AccountInfo): Future[Option[OperationView]] =
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        checkSyncStatus(accountInfo)
        for {
          operationOpt <- account.operation(uid, fullOp)
          op <- operationOpt match {
            case None => Future.successful(None)
            case Some(op) => Operations.getView(op, wallet, account).map(Some(_))
          }
        } yield op
    }

  def createAccount(accountCreationBody: AccountDerivationView, walletInfo: WalletInfo): Future[AccountView] =
    daemonCache.withWallet(walletInfo) {
      w =>
        w.addAccountIfNotExist(accountCreationBody).flatMap { a =>
          val accountInfo = AccountInfo(a.getIndex, walletInfo)
          synchronizerManager.registerAccount(a, w, accountInfo)
          val syncStatus = synchronizerManager.getSyncStatus(accountInfo).get
          a.accountView(walletInfo.walletName, w.getCurrency.currencyView, syncStatus)
        }
    }

  def createAccountWithExtendedInfo(derivations: AccountExtendedDerivationView, walletInfo: WalletInfo): Future[AccountView] =
    daemonCache.withWallet(walletInfo) {
      w =>
        w.addAccountIfNotExist(derivations).flatMap { a =>
          val accountInfo = AccountInfo(a.getIndex, walletInfo)
          synchronizerManager.registerAccount(a, w, accountInfo)
          val syncStatus = synchronizerManager.getSyncStatus(accountInfo).get
          a.accountView(walletInfo.walletName, w.getCurrency.currencyView, syncStatus)
        }
    }

  def repushOperations(accountInfo: AccountInfo, fromHeight: Option[Long]): Future[Unit] = {
    daemonCache.withAccountAndWallet(accountInfo) {
      case (account, wallet) =>
        val pushOperationFuture = account.operationsFromHeight(0, Int.MaxValue, 1, fromHeight.getOrElse(0))
          .flatMap { ops =>
            Future.sequence(ops.map(
              op =>
                Operations.getView(op, wallet, account))
            ).map(operationViews =>
              operationViews.map{opView =>
                publisher publishOperation(opView, account, wallet, accountInfo.poolName)
              }
            )
        }
        val pushErc20Future: Future[Unit] = if (account.isInstanceOfEthereumLikeAccount) {
          account.erc20Operations.flatMap { operations =>
            Future.sequence(operations.map {
              case (operation, erc20Operation) =>
                publisher.publishERC20Operation(erc20Operation, operation, account, wallet, accountInfo.poolName)
            })
          }.map(_ => Unit)
        } else Future.unit
        pushOperationFuture.flatMap(_ => pushErc20Future)
    }
  }

  private def checkSyncStatus(account: AccountInfo) = {
    synchronizerManager.getSyncStatus(account).foreach {
      case Resyncing(targetHeight, currentHeight) => throw ResyncOnGoingException(targetHeight, currentHeight)
      case _ =>
    }
  }

}

case class OperationQueryParams(previous: Option[UUID], next: Option[UUID], batch: Int, fullOp: Int)
