package co.ledger.wallet.daemon.filters

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.Request
import org.slf4j.MDC

/**
  * 
  *
  * User: Dany Rakote
  * Date: 24-01-2018
  * Time: 12:15
  *
  */

class CorrelationIdFilter[Req, Rep] extends SimpleFilter[Req, Rep] {

  override def apply(request: Req, service: Service[Req, Rep]) = {
    val correlationID = request.asInstanceOf[Request].headerMap.getOrElse("X-Correlation-ID", "")
    MDC.put("correlationId", correlationID)
    service(request)
  }
}