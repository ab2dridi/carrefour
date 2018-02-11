package carrefour.challenge.phenix.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import carrefour.challenge.phenix.caseclass._
import carrefour.challenge.phenix.utils.CmdLineParser.Config
import carrefour.challenge.phenix.utils.UsefulFunctions._

import scala.collection.immutable

object KpiTools {
  def getTop100ProduitParMagasin(splitFile: Stream[Transaction]): Map[String, List[TransactionTriplet]] = {
    splitFile.groupBy(
      p => (p.magasin, p.produit)
    ).map {
      case ((magasin: String, produit: Long), transactions: Stream[Transaction]) =>
        TransactionTriplet(magasin, produit, transactions.map(_.qte).sum)
    }.groupBy(
      p => p.magasin
    ).map {
      case (magasin: String, transactions: List[TransactionTriplet]) =>
        (magasin, transactions.sortWith(_.qte > _.qte).take(100))
    }
  }

  def getTop100ProduitGlobal(splitFile: Stream[Transaction]): Seq[TransactionTuple] = {
    splitFile.groupBy(
      p => p.produit
    ).map {
      case (produit: Long, transactions: Stream[Transaction]) =>
        TransactionTuple(produit, transactions.map(_.qte).sum)
    }.toStream.sortWith(_.qte > _.qte).take(100)
  }

  def getTop100Produit7derniersJours(
                                      formatter: DateTimeFormatter,
                                      date: LocalDate,
                                      conf: Config,
                                      currentFile: Stream[Transaction]
                                    ): Seq[TransactionTuple] = {

    val listDateToRead = (1 to 6).foldLeft(List(): List[String]) {
      (acc: List[String], minusDays: Int) => date.minusDays(minusDays).format(formatter) :: acc
    }

    val allFiles = listDateToRead.foldLeft(Stream(): Stream[Transaction]) {
      (acc: Stream[Transaction], date: String) =>
        readTransactionFile(date, conf) ++ acc
    } ++ currentFile

    getTop100ProduitGlobal(allFiles)
  }

  def getTop100CAMagasin(file: Stream[Transaction], date: String, conf: Config): Stream[TransactionTripletCA] = {
    file.groupBy(
      p => p.magasin
    ).flatMap {
      case (magasin: String, transactions: Stream[Transaction]) =>

        val mapProductWithPrice = readReferentialFile(magasin, date, conf).map {
          ref: Referentiel =>
            ref.produit -> ref.prix
        }.toMap

        transactions.map {
          p =>
            //will raise an exception if the product isnt defined in the referential
            val price = mapProductWithPrice.getOrElse(p.produit, 0.0D)
            TransactionTripletCA(magasin, p.produit, p.qte * price)
        }
    }.toStream.sortWith(_.prix > _.prix).take(100)
  }
}
