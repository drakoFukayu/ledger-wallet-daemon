package co.ledger.wallet.daemon.database.core

import co.ledger.wallet.daemon.configurations.DaemonConfiguration.CoreDbConfig
import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.{PostgresClientImpl, Row}
import com.twitter.inject.Logging
import com.twitter.util.Future

class Database(config: CoreDbConfig, poolName: String) extends Logging {
  val client: PostgresClientImpl = connectionPool

  def executeQuery[T](query: String)(f: Row => T): Future[Seq[T]] = {
    logger.info(s"SQL EXECUTION : $query")
    client.select[T](query)(f)
  }

  private def connectionPool = Postgres.Client()
    .withCredentials(config.dbUserName, config.dbPwd)
    .database(config.dbPrefix + poolName)
    .withSessionPool.maxSize(config.cnxPoolSize) // optional; default is unbounded
    .withBinaryResults(true)
    .withBinaryParams(true)
    // .withTransport.tls(config.dbHost) // TODO manage tls config option
    .newRichClient(s"${config.dbHost}:${config.dbPort}")

  def close(): Future[Unit] = client.close()
}
