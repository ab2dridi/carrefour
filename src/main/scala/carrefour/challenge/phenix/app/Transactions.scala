package carrefour.challenge.phenix.app

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import carrefour.challenge.phenix.utils.CmdLineParser._
import carrefour.challenge.phenix.utils.KpiTools._
import carrefour.challenge.phenix.caseclass._
import carrefour.challenge.phenix.utils.UsefulFunctions._
import carrefour.challenge.phenix.constant.Constants._

/**
  * la classe qui servira de classe principale de calcul des KPIs
  */

object Transactions extends App {

  parser.parse(args, Config()) map {
    conf =>
      conf.action.toLowerCase match {
        case `run` =>
          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
          val date = LocalDate.now //current date
          val formattedDate = /*date.format(formatter)*/ "20170514"

          val splitFile: Stream[Transaction] = readTransactionFile(formattedDate, conf)

          val top100ProduitParMagasin = getTop100ProduitParMagasin(splitFile)

          val top100ProduitGlobal = getTop100ProduitGlobal(splitFile)

          val top100Produit7DernierJours = getTop100Produit7derniersJours(formatter, date, conf, splitFile)

          val top100CAMagasin = getTop100CAMagasin(splitFile, formattedDate, conf)


          top100CAMagasin.foreach(println)


          //temporary return value
          top100Produit7DernierJours

        case _ =>


      }
  }

//
//  // Création d'un logger pour tracer l'application
//  val logger: Logger = Logger.getLogger(this.getClass.getName)
//
//  // calcul du temps d'exécution : déclaration d'une variable de début d'exécution du script
//  val startTime = System.nanoTime()
//
//
//  // le nom de fichier des transactions
//  val transactionsFileName = "C:\\Users\\cctt2124\\Desktop\\data\\transactions_20180211.data";
//  // le répertoire des fichiers des transactions
//  val transactionsFileNameDirectory = "C:\\Users\\cctt2124\\Desktop\\data\\"
//  val transactionsFileNamePattern = "transactions_{yyyyMMdd}.data"
//  val inputDataDelimiter = '|'
//  val daysNumber = 7
//
//
//  // generation d'une liste des fichiers pour les 7 derniers jours
//  val listesDesFichiersTransactions: Seq[String] = (0 until daysNumber).map(
//    jrDecalage => transactionsFileNamePattern.replace("{yyyyMMdd}", retourneDateDecaleNjours(jrDecalage)))
//  listesDesFichiersTransactions.foreach(println)
//
//
//  // définition d'une case classe TripletTransaction
//  case class TransactionTriplet(magasin: String, produit: Long, qte: Long)
//
//  // ouverture d'un buffer pour lire un fichier
//  val transactionsBuffer = io.Source.fromFile(transactionsFileName)
//
//  // recupération des lignes
//  val transactionsLines = transactionsBuffer.getLines()
//
//  // séparation, élimination des lignes non utilisables, casting des inputs et filtrage des données
//  val transactionsMatrix: ParSeq[TransactionTriplet] = transactionsLines.map(
//    // split des lignes par separateur
//    line => line.split(inputDataDelimiter)
//    match {
//      case Array(txId, datetime, magasin, produit, qte) => TransactionTriplet(magasin, produit.toInt, qte.toInt)
//    }
//    // élimination des lignes dont les quantités de produits commandés = 0
//  ).filter(
//    _.qte > 0
//  ).toList.par
//
//  // affichage des transactions
//  transactionsMatrix.foreach(t => println("magasin : " + t.magasin + ", produit : " + t.produit + ", quantité: " + t.qte))
//
//
//  // grouper les quantités par magasin et par produit
//  val aggregateByMagasinAndProduit: ParIterable[TransactionTriplet] =
//    transactionsMatrix
//      .groupBy(transaction => (transaction.magasin, transaction.produit))
//      .map({ case (_, v) => TransactionTriplet(v.head.magasin, v.head.produit, v.map(_.qte).sum) })
//
//  aggregateByMagasinAndProduit.foreach(println)
//
//  //
//  //
//
//  // calcul des quantités par produit (Global KPI)
//  case class TransactionProduct(produit: Int, qte: Int)
//
//  val aggregateByProduit: Array[TransactionProduct] = aggregateByMagasinAndProduit.groupBy(transaction => transaction.produit)
//    .map({
//      case (_, v) => TransactionProduct(v.head.produit, v.map(_.qte).sum)
//    }).toArray
//
//
//  val sortedListOfTopProducts: Array[TransactionProduct] = aggregateByProduit.sortWith(_.qte > _.qte).take(100)
//
//
//  import java.nio.file.{Files, Path}
//
//  Files.newDirectoryStream(path)
//    .filter(_.getFileName.toString.endsWith(".txt"))
//    .map(_.toAbsolutePath)
//
//  //
//  //
//  //
//  //    //  val aggregateByProduit = parList.groupBy(transaction => (transaction.produit)).map({
//  //     //   case(k,v) => Transaction("",v.head.produit,v.map(_.qte).sum)
//  //     // })
//  //
//  //      sortedListOfProducts.foreach(println)
//  //     // aggregateByProduit.foreach(println)
//  //
//  //
//  //      //parList.foreach(println)
//  //      //transactionsMatrix.foreach(println)
//  //
//  //
//  //
//
//  printToFile(new File("top_100_vente_GLOBAL_20170514x.data")) {
//    p: PrintWriter =>
//      sortedListOfTopProducts.foreach(
//        p.println
//      )
//  }
//
//  /*      val outputFile = "transactions_output.txt"
//        val writerBufferOutupt = new BufferedWriter(new FileWriter(outputFile))
//
//        sortedListOfProducts.foreach(writerBufferOutupt.write)
//        writerBufferOutupt.close()*/
//
//
//  transactionsBuffer.close()
//
//
//  //*********************************************
//  // **************** FIN ***********************
//  //*********************************************
//
//  val mb = 1024 * 1024
//  val runtime = Runtime.getRuntime
//
//  logger.info("Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
//  logger.info("Free Memory:  " + runtime.freeMemory / mb)
//  logger.info("Total Memory: " + runtime.totalMemory / mb)
//  logger.info("Max Memory:   " + runtime.maxMemory / mb)
//
//  // fin d'exécution du script
//  val endTime = System.nanoTime()
//  // calcul du temps total d'exécution
//  val totalExecutionTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)
//  logger.info(s" Execution time in Milliseconds: $totalExecutionTime")

}

