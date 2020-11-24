package co.ledger.wallet.daemon.database

import java.time.Instant
import java.util.Date

import co.ledger.core.{OperationType, Wallet}
import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.configurations.DaemonConfiguration.CoreDbConfig
import co.ledger.wallet.daemon.database.WalletPoolDao._
import co.ledger.wallet.daemon.models.AccountInfo
import co.ledger.wallet.daemon.models.Operations.OperationView
import co.ledger.wallet.daemon.models.coins.{BitcoinInputView, BitcoinOutputView, BitcoinTransactionView, CommonBlockView}
import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.values.ValueDecoder.instance
import com.twitter.finagle.postgres.values.{Buffers, ValueDecoder}
import com.twitter.finagle.postgres.{PostgresClientImpl, Row}
import com.twitter.inject.Logging
import com.twitter.util.{Future, Try}

import scala.concurrent.ExecutionContext

class WalletPoolDao(poolName: String)(implicit val ec: ExecutionContext) extends Logging {
  val config: CoreDbConfig = DaemonConfiguration.coreDbConfig

  val client: PostgresClientImpl = connectionPool

  type OperationUid = String

  /**
    * Partial Operation is fitting generic operation core model
    */
  case class PartialOperation(uid: String,
                              currencyName: String,
                              currencyFamily: String,
                              date: Date,
                              txHash: String,
                              blockHeight: Option[Long],
                              blockHash: Option[String],
                              blockTime: Option[Date],
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

  private val btcOperationQuery: (AccountInfo, Ordering.OperationOrder, Option[Seq[OperationUid]], Int, Int) => OperationUid =
    (accInfo: AccountInfo, order: Ordering.OperationOrder, filteredUids: Option[Seq[OperationUid]], offset: Int, limit: Int) =>
      "SELECT o.uid, o.date, bop.transaction_hash, b.height as block_height, b.time as block_time, b.hash as block_hash, o.type, o.amount, o.fees, o.senders, o.recipients " +
        "FROM accounts a, wallets w, operations o, bitcoin_operations bop, blocks b " +
        s"WHERE w.name='${accInfo.walletName}' AND a.idx='${accInfo.accountIndex}' " +
        "AND a.wallet_uid=w.uid AND o.account_uid=a.uid AND o.uid = bop.uid AND o.block_uid=b.uid " +
        filteredUids.fold("")(uids => s"AND o.uid IN ('${uids.mkString("','")}') ") +
        "ORDER BY o.date " + order.value +
        s" OFFSET $offset LIMIT $limit"

  /**
    * List operations from an account
    */
  def listAllOperations(a: AccountInfo, w: Wallet, offset: Int, limit: Int): Future[Seq[OperationView]] = {
    listOperations(a, w, None, offset, limit)
  }

  /**
    * Find Operation
    */
  def findOperationByUid(a: AccountInfo, w: Wallet, uid: OperationUid, offset: Int, limit: Int): Future[Option[OperationView]] = {
    listOperations(a, w, Some(Seq(uid)), offset, limit).map(_.headOption)
  }

  /**
    * List operations from an account
    */
  def listOperations(a: AccountInfo, w: Wallet, filteredUids: Option[Seq[OperationUid]], offset: Int, limit: Int): Future[Seq[OperationView]] = {
    logger.info(s"Retrieving operations for account : $a - limit=$limit offset=$offset")
    val currency = w.getCurrency
    val currencyName = currency.getName
    val currencyFamily = currency.getWalletType
    var uids = Seq[OperationUid]()

    def retrievePartialOperations = {
      queryBitcoinOperations[PartialOperation](a, filteredUids, offset, limit) {
        row => {
          val opUid = row.get[String]("uid")
          uids = uids :+ opUid
          PartialOperation(
            opUid, currencyName, currencyFamily.name(),
            row.get[Date]("date"),
            row.get[String]("transaction_hash"),
            row.getOption[Long]("block_height"),
            row.getOption[String]("block_hash"),
            row.getOption[Date]("block_time"),
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
      val opIntputs = inputs.groupBy(_._1).map { case (opUid, pair) => (opUid, pair.map(_._2)) }
      val opOutputs = outputs.groupBy(_._1).map { case (opUid, pair) => (opUid, pair.map(_._2)) }

      operations.map(pop => {
        val confirmations = 0 // pop.blockHeight.fold(0L)(opHeight => (lastBlock.getHeight - opHeight) + 1)
        val txView = (opIntputs.get(pop.uid), opOutputs.get(pop.uid)) match {
          case (Some(inputs), Some(outputs)) =>
            val blockView = (pop.blockHash, pop.blockHeight, pop.blockTime) match {
              case (Some(hash), Some(height), Some(time)) => Some(CommonBlockView(hash, height, time))
              case _ => None
            }
            Some(BitcoinTransactionView(blockView, Some(pop.fees.toString()), pop.txHash, pop.date, inputs, 0L, outputs))
          case _ => None
        }
        OperationView(pop.uid, currencyName, currencyFamily, None, confirmations,
          pop.date,
          pop.blockHeight,
          OperationType.valueOf(pop.opType),
          pop.amount.toString(), pop.fees.toString(), a.walletName, a.accountIndex, pop.senders, pop.recipients, Seq.empty, txView)

      })
    }
  }

  // TODO : offset / limit for resilience
  def findBitcoinOutputs(opUids: Seq[OperationUid]): Future[Seq[(OperationUid, BitcoinOutputView)]] =
    queryBitcoinOutputs[(OperationUid, BitcoinOutputView)](opUids, 0, Int.MaxValue) {
      row => (row.get[String]("uid"), BitcoinOutputView(
          row.get[String]("address"),
          row.get[String]("hash"),
          row.get[Int]("idx"),
          row.get[String]("amount"),
          row.get[String]("script"))
      )
    }

  def findBitcoinInputs(opUids: Seq[OperationUid]): Future[Seq[(OperationUid, BitcoinInputView)]] = {
    queryBitcoinInputs[(OperationUid, BitcoinInputView)](opUids, 0, Int.MaxValue) {
      row => {
        val opUid = row.get[String]("uid")
        val outputView = BitcoinInputView(
          row.get[String]("address"),
          row.get[String]("amount"),
          row.getOption[String]("coinbase"),
          row.getOption[String]("previous_tx_hash"),
          row.getOption[Int]("previous_output_idx")
        )
        (opUid, outputView)
      }
    }
  }

  private def queryBitcoinOutputs[T](opUids: Seq[OperationUid], offset: Int, limit: Int)(f: Row => T) = {
    executeQuery[T](btcOutputQuery(opUids, offset, limit))(f)
  }

  private def queryBitcoinInputs[T](opUids: Seq[OperationUid], offset: Int, limit: Int)(f: Row => T) = {
    executeQuery[T](btcInputQuery(opUids, offset, limit))(f)
  }

  private def queryBitcoinOperations[T](accInfo: AccountInfo, filteredUids: Option[Seq[OperationUid]], offset: Int, limit: Int)(f: Row => T) = {
    executeQuery[T](btcOperationQuery(accInfo, Ordering.Ascending, filteredUids, offset, limit))(f)
  }

  def executeQuery[T](query: String)(f: Row => T): Future[Seq[T]] = {
    logger.info(s"SQL EXECUTION : $query")
    client.select[T](query)(f)
  }
}

object WalletPoolDao {
  def apply(name: String)(implicit ec: ExecutionContext): WalletPoolDao = new WalletPoolDao(name)

  import java.text.SimpleDateFormat

  val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

  def parseDate(dateStr: String): Date = {
    Date.from(Instant.parse(dateStr))
  }

  implicit val dateDecoder: ValueDecoder[Date] = instance(
    s => Try {
      parseDate(s)
    },
    (b, c) => Try(parseDate(Buffers.readString(b, c)))
  )

  object Ordering {

    sealed trait OperationOrder {
      val value: String
    }

    case object Ascending extends OperationOrder {
      override val value: String = "ASC"
    }

    case object Descending extends OperationOrder {
      override val value: String = "DSC"
    }

  }

}
