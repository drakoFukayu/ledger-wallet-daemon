package co.ledger.wallet.daemon.database

import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext.Implicits.global

import co.ledger.wallet.daemon.models.{AccountInfo, Pool}
import co.ledger.wallet.daemon.utils.NativeLibLoader
import com.twitter.inject.Logging
import com.twitter.util.{Await => TwitterAwait}
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

import scala.concurrent.Await
import scala.concurrent.duration.Duration


@Test
class WalletPoolDAOTest extends AssertionsForJUnit with Logging {
  NativeLibLoader.loadLibs()

  @Test def testConnectionToDB(): Unit = {
    val walletName = "bitcoin"
    val poolName = "steltest"

    val pool = Pool.newPoolInstance(PoolDto(poolName, "", Some(1))).get
    val wallet = Await.result(pool.wallet("bitcoin"), Duration.Inf).get

    val poolDao = new WalletPoolDao(poolName)
    val allOperations = TwitterAwait.result(poolDao.listOperations(AccountInfo(0, walletName, poolName), wallet, 0, 10))
    logger.info(s" All operations : $allOperations")
  }

}
