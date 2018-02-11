package carrefour.challenge.phenix.utils

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import carrefour.challenge.phenix.caseclass._
import carrefour.challenge.phenix.utils.CmdLineParser.Config
import carrefour.challenge.phenix.utils.UsefulFunctions._

object KpiTools {

  /**
    *
    * @param splitFile
    * @return
    */
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


  /**
    *
    * @param splitFile
    * @return
    */
  def getTop100ProduitParMagasinAndWriteOutput(splitFile: Stream[Transaction]): Map[String, List[TransactionTriplet]] = {
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

  /**
    *
    * @param splitFile
    * @return
    */
  def getTop100ProduitGlobal(splitFile: Stream[Transaction]): Seq[TransactionTuple] = {
    splitFile.groupBy(
      p => p.produit
    ).map {
      case (produit: Long, transactions: Stream[Transaction]) =>
        TransactionTuple(produit, transactions.map(_.qte).sum)
    }.toStream.sortWith(_.qte > _.qte).take(100)
  }

  /**
    *
    * @param formatter
    * @param date
    * @param conf
    * @param currentFile
    * @return
    */
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

  def getTop100Produit7derniersJoursParMagasin(
                                                formatter: DateTimeFormatter,
                                                date: LocalDate,
                                                conf: Config,
                                                currentFile: Stream[Transaction]
                                              ): Map[String, List[TransactionTriplet]] = {

    val listDateToRead = (1 to 6).foldLeft(List(): List[String]) {
      (acc: List[String], minusDays: Int) => date.minusDays(minusDays).format(formatter) :: acc
    }

    val allFiles = listDateToRead.foldLeft(Stream(): Stream[Transaction]) {
      (acc: Stream[Transaction], date: String) =>
        readTransactionFile(date, conf) ++ acc
    } ++ currentFile

    getTop100ProduitParMagasin(allFiles)
  }


  /**
    *
    * @param file
    * @param date
    * @param conf
    * @return
    */
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
    }.groupBy(
      p => (p.magasin, p.produit)
    ).map {
      case ((magasin: String, produit: Long), transactions: List[TransactionTripletCA]) =>
        TransactionTripletCA(magasin, produit, transactions.map(_.prix).sum)
    }.toStream.sortWith(_.prix > _.prix).take(100)
  }

  def getTop100CAGlobal(file: Stream[Transaction], date: String, conf: Config): Stream[TransactionTupleCA] = {
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
            TransactionTupleCA(p.produit, p.qte * price)
        }
    }.groupBy(
      p => p.produit
    ).map {
      case (produit: Long, transactions: List[TransactionTupleCA]) =>
        TransactionTupleCA(produit, transactions.map(_.prix).sum)
    }.toStream.sortWith(_.prix > _.prix).take(100)
  }

  def getTop100CA7derniersJoursParMagasin(
                                           formatter: DateTimeFormatter,
                                           date: LocalDate,
                                           conf: Config
                                         ): Stream[TransactionTripletCA] = {

    val listDateToRead = (0 to 1).foldLeft(List(): List[String]) {
      (acc: List[String], minusDays: Int) => date.minusDays(minusDays).format(formatter) :: acc
    }

    val allFiles = listDateToRead.foldLeft(Stream(): Stream[JoinedTransaction]) {
      (acc: Stream[JoinedTransaction], date: String) =>
        val transactions = readTransactionFile(date, conf)
        transactions.map {
          transaction: Transaction =>
            val mapProductWithPrice = readReferentialFile(transaction.magasin, date, conf).map {
              ref: Referentiel =>
                ref.produit -> ref.prix
            }.toMap

            JoinedTransaction(
              transaction.magasin,
              transaction.produit,
              transaction.qte,
              mapProductWithPrice(transaction.produit)
            )
        } ++ acc
    }

    allFiles.groupBy(
      p => p.magasin
    ).flatMap {
      case (magasin: String, transactions: Stream[JoinedTransaction]) =>
        transactions.map {
          jtransaction =>
            TransactionTripletCA(magasin, jtransaction.produit, jtransaction.qte * jtransaction.prix)
        }
    }.groupBy(
      p => (p.magasin, p.produit)
    ).map {
      case ((magasin: String, produit: Long), transactions: List[TransactionTripletCA]) =>
        TransactionTripletCA(magasin, produit, transactions.map(_.prix).sum)
    }.toStream.sortWith(_.prix > _.prix).take(100)
  }

  def getTop100CA7derniersJoursGlobal(
                                       formatter: DateTimeFormatter,
                                       date: LocalDate,
                                       conf: Config
                                     ): Stream[TransactionTupleCA] = {

    val listDateToRead = (0 to 1).foldLeft(List(): List[String]) {
      (acc: List[String], minusDays: Int) => date.minusDays(minusDays).format(formatter) :: acc
    }

    val allFiles = listDateToRead.foldLeft(Stream(): Stream[JoinedTransaction]) {
      (acc: Stream[JoinedTransaction], date: String) =>
        val transactions = readTransactionFile(date, conf)
        transactions.map {
          transaction: Transaction =>
            val mapProductWithPrice = readReferentialFile(transaction.magasin, date, conf).map {
              ref: Referentiel =>
                ref.produit -> ref.prix
            }.toMap

            JoinedTransaction(
              transaction.magasin,
              transaction.produit,
              transaction.qte,
              mapProductWithPrice(transaction.produit)
            )
        } ++ acc
    }

    allFiles.groupBy(
      p => p.produit
    ).flatMap {
      case (produit: Long, transactions: Stream[JoinedTransaction]) =>
        transactions.map {
          jtransaction =>
            TransactionTupleCA(produit, jtransaction.qte * jtransaction.prix)
        }
    }.groupBy(
      p => p.produit
    ).map {
      case (produit: Long, transactions: List[TransactionTupleCA]) =>
        TransactionTupleCA(produit, transactions.map(_.prix).sum)
    }.toStream.sortWith(_.prix > _.prix).take(100)
  }
}
