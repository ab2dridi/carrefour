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
    * fonction qui permet d'écrire un fichier en sortie
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

  /**
    * fonction qui permet de lire et de parser le fichier transaction par date
    * @param date la date courante (utilisée pour reconstruire le nom de fichier de transactions à lire
    * @param conf le repertoire du fichier
    * @return retourne un Stream de Transactions
    */
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


  /**
    * fonction qui permet de lire et de parser le fichier référentiel par magasin et par date
    * @param idMagasin
    * @param date
    * @param config
    * @return retourne un stream de referentiel par magasion et par date
    */
  def readReferentialFile(idMagasin: String, date: String, config: Config): Stream[Referentiel] = {
    // on construit le nom du fichier à lire
    val fileName: String = getReferentialFileName(idMagasin, date, config)

    // lecture en Stram
    val referentialFile: Stream[String] = Source.fromFile(fileName).getLines().toStream

    // pour chaque ligne du fichier on la split selon le séparateur
    // et on parse l'input
    referentialFile map {
      row =>
        val splittedRow: Array[String] = row.split('|')
        Referentiel(
          splittedRow(0).toLong,
          splittedRow(1).toDouble
        )
    }
  }

  /**
    * fonction qui permet de retourner le nom du répertoire des fichiers sans le caractère (/) vers la fin
    * @param conf
    * @return
    */
  def computeDirectoryPath(conf: Config): String = if (conf.fileDirectory.endsWith("/"))
    conf.fileDirectory.dropRight(1)
  else conf.fileDirectory

  /**
    * fonction qui permet de construire un nom de fichier Transactions
    * (concat le repertoire+PatternNomDuFichier+date.data)
    * @param date la date du fichier à lire
    * @param config le repertoire du fichier
    * @return retourne le chemin complet du fichier
    */
  def getTransactionFileName(date: String, config: Config): String = {
    val directory: String = computeDirectoryPath(config)
    s"$directory/transactions_$date.data"
  }

  /**
    * fonction qui permet de construire un nom de fichier Réferentiel par magasin
    * (concat le répertoire+PatternNomDufichier+IdMagasin+date.data)
    * @param idMagasin
    * @param date
    * @param config
    * @return retourne le nom du fichier referentiel
    */
  def getReferentialFileName(idMagasin: String, date: String, config: Config): String = {
    val directory: String = computeDirectoryPath(config)
    s"$directory/reference_prod-${idMagasin}_$date.data"
  }


  /**
    * permet de retourner des informations concernant la consommation mémoire actuelle
    * @param runtime
    * @return
    */
  def getMemoryInformations(logger: java.util.logging.Logger) = {
    val runtime = Runtime.getRuntime
    val mb = 1024 * 1024
    logger.info("Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    logger.info("Free Memory:  " + runtime.freeMemory / mb)
    logger.info("Total Memory: " + runtime.totalMemory / mb)
    logger.info("Max Memory:   " + runtime.maxMemory / mb)
  }
}


