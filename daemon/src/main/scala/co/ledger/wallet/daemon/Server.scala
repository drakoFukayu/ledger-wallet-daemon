package co.ledger.wallet.daemon

import co.ledger.wallet.daemon.controllers._
import co.ledger.wallet.daemon.filters._
import co.ledger.wallet.daemon.mappers.AuthenticationExceptionMapper
import co.ledger.wallet.daemon.modules.{DaemonCacheModule, DaemonJacksonModule}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{AccessLoggingFilter, CommonFilters}
import com.twitter.finatra.http.routing.HttpRouter
import djinni.NativeLibLoader

object Server extends ServerImpl {

}

class ServerImpl extends HttpServer {

  override def jacksonModule = DaemonJacksonModule

  override val modules = Seq(DaemonCacheModule)

  override protected def configureHttp(router: HttpRouter): Unit =
    router
      .filter[LoggingMDCFilter[Request, Response]]
      .filter[TraceIdMDCFilter[Request, Response]]
      .filter[CorrelationIdFilter[Request, Response]]
      .filter[CommonFilters]
      .filter[AccessLoggingFilter[Request]]
      .filter[DemoUserAuthenticationFilter]
      .filter[LWDAutenticationFilter]
      .add[AuthenticationFilter, AccountsController]
      .add[AuthenticationFilter, CurrenciesController]
      .add[StatusController]
      .add[AuthenticationFilter, WalletPoolsController]
      .add[AuthenticationFilter, WalletsController]
      .add[AuthenticationFilter, TransactionsController]
      .exceptionMapper[AuthenticationExceptionMapper]

  override protected def warmup(): Unit = {
    super.warmup()
    NativeLibLoader.loadLibs()
  }
}
