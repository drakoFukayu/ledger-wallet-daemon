package co.ledger.wallet.daemon.database

import java.time.LocalDateTime

import co.ledger.core.Wallet
import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.configurations.DaemonConfiguration.CoreDbConfig
import co.ledger.wallet.daemon.models.AccountInfo
import co.ledger.wallet.daemon.models.coins.{BitcoinInputView, BitcoinOutputView}
import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.{PostgresClientImpl, Row}
import com.twitter.inject.Logging
import com.twitter.util.Future

class WalletPoolDao(poolName: String) extends Logging {
  val config: CoreDbConfig = DaemonConfiguration.coreDbConfig

  val client: PostgresClientImpl = connectionPool

  type OperationUid = String

  /**
    * Partial Operation is fitting generic operation core model
    */
  case class PartialOperation(uid: String,
                              currencyName: String,
                              currencyFamily: String,
                              time: LocalDateTime,
                              blockHeight: Option[Long],
                              opType: String,
                              amount: BigInt,
                              fees: BigInt,
                              walletName: String,
                              accountIndex: Int,
                              senders: Seq[String],
                              recipients: Seq[String])


  private def connectionPool = Postgres.Client()
    .withCredentials(config.dbUserName, config.dbPwd)
    .database(config.dbPrefix + poolName)
    .withSessionPool.maxSize(config.cnxPoolSize) // optional; default is unbounded
    .withBinaryResults(true)
    .withBinaryParams(true)
    // .withTransport.tls(config.dbHost) // TODO manage tls config option
    .newRichClient(s"${config.dbHost}:${config.dbPort}")

  private val btcInputQuery: (Seq[OperationUid], Int, Int) => OperationUid = (opUids: Seq[OperationUid], offset: Int, limit: Int) =>
    "SELECT bop.uid, bins.address, bins.amount, bins.coinbase, bins.previous_tx_hash, bins.previous_output_idx " +
      "FROM bitcoin_operations bop, bitcoin_transactions bt, bitcoin_inputs bins, bitcoin_transaction_inputs btxi " +
      "WHERE bop.transaction_uid = bt.transaction_uid " +
      "AND btxi.transaction_uid = bt.transaction_uid " +
      "AND btxi.input_uid = bins.uid " +
      s"AND bop.uid IN ('${opUids.mkString("','")}') " +
      s"OFFSET $offset LIMIT $limit"

  private val btcOutputQuery: (Seq[OperationUid], Int, Int) => OperationUid = (opUids: Seq[OperationUid], offset: Int, limit: Int) =>
    "SELECT bop.uid, bt.hash, bout.idx, bout.amount, bout.address, bout.script " +
      "FROM bitcoin_operations bop, bitcoin_transactions bt, bitcoin_outputs bout " +
      "WHERE bt.transaction_uid = bop.transaction_uid " +
      "AND bout.transaction_uid = bt.transaction_uid " +
      s"AND bop.uid IN ('${opUids.mkString("','")}') " +
      s"OFFSET $offset LIMIT $limit"

  private val btcOperationQuery: (AccountInfo, Int, Int) => OperationUid = (accInfo: AccountInfo, offset: Int, limit: Int) =>
    "SELECT o.uid, o.date, b.height, o.type, o.amount, o.fees, o.senders, o.recipients " +
    "FROM accounts a, wallets w, operations o, blocks b " +
    s"WHERE w.name='${accInfo.walletName}' AND a.idx='${accInfo.accountIndex}' " +
    "AND a.wallet_uid=w.uid AND o.account_uid=a.uid AND o.block_uid=b.uid " +
    s"OFFSET $offset LIMIT $limit"

  /**
    * List operations from an account
    */
  def listOperations(a: AccountInfo, w: Wallet, offset: Int, limit: Int): Future[Seq[PartialOperation]] = {
    logger.info(s"Retrieving operations for account : $a - limit=$limit offset=$offset")
    val currency = w.getCurrency
    val currencyName = currency.getName
    val currencyFamily = currency.getWalletType.name()
    var uids = Seq[OperationUid]()

    def retrievePartialOperations = {
      queryBitcoinOperations[PartialOperation](a, offset, limit) {
        row => {
          val opUid = row.get[String]("uid")
          uids = uids :+ opUid
          PartialOperation(
            opUid, currencyName, currencyFamily,
            row.get[LocalDateTime]("date"),
            row.getOption[Long]("height"),
            row.get[String]("type"),
            BigInt(row.get[String]("amount"), 16),
            BigInt(row.get[String]("fees"), 16),
            a.walletName, a.accountIndex,
            row.get[String]("senders").split(','),
            row.get[String]("recipients").split(',')
          )
        }
      }
    }

    for {
      operations <- retrievePartialOperations
      outputs <- findBitcoinOutputs(uids)
      inputs <- findBitcoinInputs(uids)
    } yield {
      logger.info(s"Found outputs : $outputs")
      logger.info(s"Found inputs : $inputs")
      operations
    }
  }

  def executeQuery[T](query: String)(f: Row => T): Future[Seq[T]] = {
    logger.info(s"SQL EXECUTION : $query")
    client.select[T](query)(f)
  }

  // TODO : offset / limit for resilience
  def findBitcoinOutputs(opUids: Seq[OperationUid]): Future[Seq[(OperationUid, BitcoinOutputView)]] = {
    queryBitcoinOutputs[(OperationUid, BitcoinOutputView)](opUids, 0, Int.MaxValue)(row => {
      val opUid = row.get[String]("uid")
      val outputView = BitcoinOutputView(
        row.get[String]("address"),
        row.get[String]("hash"),
        row.get[Int]("idx"),
        row.get[String]("amount"),
        row.get[String]("script"),
      )
      (opUid, outputView)
    })
  }

  def findBitcoinInputs(opUids: Seq[OperationUid]): Future[Seq[(OperationUid, BitcoinInputView)]] = {
    queryBitcoinInputs[(OperationUid, BitcoinInputView)](opUids, 0, Int.MaxValue)(row => {
      val opUid = row.get[String]("uid")
      val outputView = BitcoinInputView(
        row.get[String]("address"),
        row.get[String]("amount"),
        row.getOption[String]("coinbase"),
        row.getOption[String]("previous_tx_hash"),
        row.getOption[Int]("previous_output_idx")
      )
      (opUid, outputView)
    })
  }

  private def queryBitcoinOutputs[T](opUids: Seq[OperationUid], offset: Int, limit: Int)(f: Row => T) = {
    executeQuery[T](btcOutputQuery(opUids, offset, limit))(f)
  }

  private def queryBitcoinInputs[T](opUids: Seq[OperationUid], offset: Int, limit: Int)(f: Row => T) = {
    executeQuery[T](btcInputQuery(opUids, offset, limit))(f)
  }

  private def queryBitcoinOperations[T](accInfo: AccountInfo, offset: Int, limit: Int)(f: Row => T) = {
    executeQuery[T](btcOperationQuery(accInfo, offset, limit))(f)
  }
}
