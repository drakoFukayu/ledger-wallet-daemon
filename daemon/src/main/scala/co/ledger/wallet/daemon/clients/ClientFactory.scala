package co.ledger.wallet.daemon.clients

import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext
import co.ledger.wallet.daemon.libledger_core.async.ScalaThreadDispatcher

import scala.concurrent.ExecutionContext


object ClientFactory {
  implicit val ec: ExecutionContext = MDCPropagatingExecutionContext.Implicits.global

  lazy val webSocketClient = new ScalaWebSocketClient
  lazy val httpClient = new ScalaHttpClient
  lazy val threadDispatcher = new ScalaThreadDispatcher(ec)
}
