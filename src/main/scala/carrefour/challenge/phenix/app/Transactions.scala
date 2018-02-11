package carrefour.challenge.phenix.app

import java.util.logging.Logger
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import carrefour.challenge.phenix.utils.CmdLineParser._
import carrefour.challenge.phenix.utils.KpiTools._
import carrefour.challenge.phenix.caseclass._
import carrefour.challenge.phenix.utils.UsefulFunctions._
import carrefour.challenge.phenix.constant.Constants._
import java.io._


/**
  * la classe qui servira de classe principale de calcul des KPIs
  */

object Transactions extends App {

  parser.parse(args, Config()) map {
    conf =>
      conf.action.toLowerCase match {
        case `run` =>

          val logger: Logger = Logger.getLogger(this.getClass.getName)

          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
          val date = LocalDate.now //current date
          val formattedDate = /*date.format(formatter)*/ "20170514"

          val splitFile: Stream[Transaction] = readTransactionFile(formattedDate, conf)

          // 1. indicateur : top_100_ventes_<MAGASIN_ID>_YYYYMMDD.data
          val top100ProduitParMagasin: Map[String, List[TransactionTriplet]] = getTop100ProduitParMagasin(splitFile)

          top100ProduitParMagasin foreach {
            case (magasinId:String, transactions : List[TransactionTriplet]) =>
              // stockage de l'indicateur en output
              printToFile(new File(s"top_100_ventes_GLOBAL_${magasinId}_$formattedDate.data")) {
                p: PrintWriter =>
                  p.println("produit, qte")
                  transactions.foreach{
                    transaction : TransactionTriplet =>
                      p.println(s"${transaction.produit}, ${transaction.qte}")
                  }
              }
          }

          // 2. indicateur : top_100_ventes_GLOBAL_YYYYMMDD.data
          val top100ProduitGlobal: Seq[TransactionTuple] = getTop100ProduitGlobal(splitFile)

          // stockage de l'indicateur en output
          printToFile(new File(s"top_100_ventes_GLOBAL_$formattedDate.data")) {
            p: PrintWriter =>
              p.println("produit, qte")
              top100ProduitGlobal.foreach{
                transaction : TransactionTuple =>
                p.println(s"${transaction.produit}, ${transaction.qte}")
                }
              }


          // 3. indicateur : top_100_ca_<MAGASIN_ID>_YYYYMMDD.data
          val top100CAMagasin = getTop100CAMagasin(splitFile, formattedDate, conf)

          //4. indicateur: top_100_ca_GLOBAL_YYYYMMDD.data
          val top100CAGlobal = getTop100CAGlobal(splitFile, formattedDate, conf)

          //5. indicateur: top_100_ventes_<MAGASIN_ID>_YYYYMMDD-J7.data
          val top100ProduitsJ7 = getTop100Produit7derniersJoursParMagasin(formatter, date, conf, splitFile)

          //6. indicateur : top_100_ventes_GLOBAL_YYYYMMDD-J7.data
          val top100Produit7DernierJours = getTop100Produit7derniersJours(formatter, date, conf, splitFile)

          //7. indicateur: top_100_ca_<MAGASIN_ID>_YYYYMMDD-J7.data
          val top100CAMagasinJ7 = getTop100CA7derniersJoursParMagasin(formatter, date, conf)

          //8. indicateur: top_100_ca_GLOBAL_YYYYMMDD-J7.data
          val top100CAGlobalJ7 = getTop100CA7derniersJoursGlobal(formatter, date, conf)


          top100CAGlobal.foreach(println)
//          top100ProduitsJ7.foreach(println)
//          top100CAMagasinJ7.foreach(println)
//          top100CAGlobalJ7.foreach(println)

          System.exit(0)


          // stockage de l'indicateur en output
          printToFile(new File(s"top_100_ventes_GLOBAL_$formattedDate-J7.data")) {
            p: PrintWriter =>
              top100Produit7DernierJours.foreach(
                p.println
              )}


          top100CAMagasin.foreach(println)


          //temporary return value
          top100Produit7DernierJours






          // get memory informations
          getMemoryInformations(logger)


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

