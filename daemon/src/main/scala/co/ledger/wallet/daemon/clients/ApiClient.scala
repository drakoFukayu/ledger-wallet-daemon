package co.ledger.wallet.daemon.clients

import javax.inject.Singleton

import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import co.ledger.wallet.daemon.models.FeeMethod
import co.ledger.wallet.daemon.utils._
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.finagle.Http
import com.twitter.finagle.http.{Method, Request}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Client for request to blockchain explorers.
  *
  * User: Ting Tu
  * Date: 24-04-2018
  * Time: 14:32
  *
  */
@Singleton
class ApiClient(implicit val ec: ExecutionContext) {
  import ApiClient._
  private[this] val (host, port, poolSize) = DaemonConfiguration.apiConnection
  private [this] val client = Http.client
    .withSessionPool.maxSize(poolSize)
    .newService(s"$host:$port")

  //TODO: support dynamically
  def getFees(currencyName: String): Future[FeeInfo] = {
    val path = currencyName match {
      case "bitcoin" => "/blockchain/v2/btc/fees"
      case "bitcoin_cash" => "/blockchain/v2/abc/fees"
      case "bitcoin_testnet" => "/blockchain/v2/btc_testnet/fees"
      case "dogecoin" => "/blockchain/v2/doge/fees"
      case "litecoin" => "/blockchain/v2/ltc/fees"
      case "dash" => "/blockchain/v2/dash/fees"
      case "komodo" => "/blockchain/v2/kmd/fees"
      case "pivx" => "/blockchain/v2/pivx/fees"
      case "viacoin" => "/blockchain/v2/via/fees"
      case "vertcoin" => "/blockchain/v2/vtc/fees"
      case "digibyte" => "/blockchain/v2/dgb/fees"
      case _ => throw new UnsupportedOperationException(s"currency not supported '$currencyName'")
    }
    val request = Request(Method.Get, path)
    request.host = "api.ledgerwallet.com"
    client(request).map { response =>
      mapper.readValue(response.contentString, classOf[FeeInfo])
    }.asScala
  }

  private val mapper: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
}

object ApiClient {
  case class FeeInfo(
                    @JsonProperty("1") fast: Long,
                    @JsonProperty("3") normal: Long,
                    @JsonProperty("6") slow: Long) {

    def getAmount(feeMethod: FeeMethod): Long = feeMethod match {
      case FeeMethod.FAST => fast / 1000
      case FeeMethod.NORMAL => normal / 1000
      case FeeMethod.SLOW => slow / 1000
      case _ => throw new UnsupportedOperationException(s"Fee method not supported '$feeMethod'")
    }
  }
}
