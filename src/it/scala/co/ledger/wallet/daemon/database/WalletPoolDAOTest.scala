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
    val allOperations = TwitterAwait.result(poolDao.listAllOperations(AccountInfo(0, walletName, poolName), wallet, 0, 100))
    logger.info(s" All operations : ${allOperations.size}")

    val filteredOperations = TwitterAwait.result(
      poolDao.listOperations(AccountInfo(0, walletName, poolName), wallet, Some(Seq("27c029b32592262ec035bffbea0f7050aa4440436174358c4a3877578509365e", "4950c9d8f7aaafc770af77ba098bb0a2e73e64152b1e2d490aa0d203ade98255")), 0, 100))
    logger.info(s" Filtered operations : ${filteredOperations.size}")
  }

}
