package carrefour.challenge.phenix.utils

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar

import carrefour.challenge.phenix.caseclass.{Referentiel, Transaction}
import carrefour.challenge.phenix.utils.CmdLineParser.Config

import scala.io.Source

object UsefulFunctions {


  /**
    *
    * @param n nombre de jours de déclage par rapport à la date actuelle
    * @return retourne la date courante au format yyyyMMdd décalée de n jours
    */

  def retourneDateDecaleNjours(n: Int): String = {
    val nbJoursDecalage = 0 - n
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, nbJoursDecalage)
    dateFormat.format(cal.getTime)
  }


  /**
    *
    * @param f
    * @param op
    */
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  def readTransactionFile(date: String, conf: Config): Stream[Transaction] = {
    val transactionFilename = getTransactionFileName(date, conf)

    val transactionFile = Source.fromFile(transactionFilename).getLines().toStream

    transactionFile map {
      row =>
        val splitRow = row.split('|')
        Transaction(
          splitRow(0).toLong,
          splitRow(1) /*LocalDate.parse(splitRow(1), formatter)*/ , //todo : cast as date
          splitRow(2),
          splitRow(3).toLong,
          splitRow(4).toLong
        )
    }
  }


  def readReferentialFile(idMagasin: String, date: String, config: Config): Stream[Referentiel] = {
    val fileName = getReferentialFileName(idMagasin, date, config)

    val referentialFile = Source.fromFile(fileName).getLines().toStream

    referentialFile map {
      row =>
        val splitRow = row.split('|')
        Referentiel(
          splitRow(0).toLong,
          splitRow(1).toDouble
        )
    }
  }

  def computeDirectoryPath(conf: Config): String = if (conf.fileDirectory.endsWith("/"))
    conf.fileDirectory.dropRight(1)
  else conf.fileDirectory

  def getTransactionFileName(date: String, config: Config): String = {
    val directory = computeDirectoryPath(config)
    s"$directory/transactions_$date.data"
  }

  def getReferentialFileName(idMagasin: String, date: String, config: Config): String = {
    val directory = computeDirectoryPath(config)
    s"$directory/reference_prod-${idMagasin}_$date.data"
  }
}
