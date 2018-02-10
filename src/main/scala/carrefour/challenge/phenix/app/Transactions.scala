package carrefour.challenge.phenix.app

import java.util.logging.Logger
import java.util.Calendar
import java.util.concurrent.TimeUnit
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.io._

import carrefour.challenge.phenix.utilities.UsefulFunctions._

import scala.collection.parallel.immutable.{ParIterable, ParSeq}
import scala.io.Source

/**
  * la classe qui servira de classe principale de calcul des KPIs
  */

  object Transactions {
    def main(args: Array[String]) : Unit = {

      // Création d'un logger pour tracer l'application
      val logger: Logger = Logger.getLogger(this.getClass.getName)

      // check arguments first
      if (args.length==0)
      {
        logger severe "usage : scala -classpath CarrefourChallenge-0.1.jar carrefour.challenge.phenix.App.Transactions input_files_directory \n" +
          "infile : the input files directory (transactions files and referentiels files)\n" +
          "outfile : KPIs files directory";
        System.exit(1);
      }


      // calcul du temps d'exécution : déclaration d'une variable de début d'exécution du script
      val startTime = System.nanoTime()


      // le nom de fichier des transactions
      val transactionsFileName = "C:\\Users\\cctt2124\\Desktop\\data\\transactions_20170514.data";
      // le répertoire des fichiers des transactions
      val transactionsFileNameDirectory="C:\\Users\\cctt2124\\Desktop\\data\\"
      val transactionsFileNamePattern = "transactions_{yyyyMMdd}.data"
      val inputDataDelimiter = '|'
      val daysNumber=7


        // generation d'une liste des fichiers pour les 7 derniers jours
      val listesDesFichiersTransactions: Seq[String] = (0 to daysNumber-1).map(
        jrDecalage => transactionsFileNamePattern.replace("{yyyyMMdd}",retourneDateDecaleNjours(jrDecalage)))
      listesDesFichiersTransactions.foreach(println)



      // définition d'une case classe TripletTransaction
      case class TransactionTriplet (magasin: String, produit: Int, qte: Int)

      // ouverture d'un buffer pour lire un fichier
      val transactionsBuffer = io.Source.fromFile(transactionsFileName)

      // recupération des lignes
      val transactionsLines = transactionsBuffer.getLines()

      // séparation, élimination des lignes non utilisables, casting des inputs et filtrage des données
      val transactionsMatrix: ParSeq[TransactionTriplet] = transactionsLines.map(
        // split des lignes par separateur
        line => line.split(inputDataDelimiter)
        match {
          case Array(txId, datetime, magasin, produit, qte) => TransactionTriplet(magasin, produit.toInt, qte.toInt)
        }
        // élimination des lignes dont les quantités de produits commandés = 0
      ).filter(
        _.qte>0
      ).toList.par

      // affichage des transactions
      transactionsMatrix.foreach(t => println("magasin : "+ t.magasin+", produit : "+t.produit+", quantité: "+t.qte))



      // grouper les quantités par magasin et par produit
     val aggregateByMagasinAndProduit: ParIterable[TransactionTriplet] =
       transactionsMatrix
        .groupBy(transaction => (transaction.magasin,transaction.produit))
        .map({case(_,v) => TransactionTriplet(v.head.magasin,v.head.produit,v.map(_.qte).sum)})

      aggregateByMagasinAndProduit.foreach(println)
//
//

      // calcul des quantités par produit (Global KPI)
      case class TransactionProduct (produit: Int, qte: Int)

      val aggregateByProduit: Array[TransactionProduct] = aggregateByMagasinAndProduit.groupBy(transaction => transaction.produit)
        .map({
          case(_,v)=> TransactionProduct(v.head.produit,v.map(_.qte).sum)
        }).toArray


     val sortedListOfTopProducts: Array[TransactionProduct] = aggregateByProduit.sortWith(_.qte>_.qte).take(100)


      import java.nio.file.{Files, Path}
      Files.newDirectoryStream(path)
        .filter(_.getFileName.toString.endsWith(".txt"))
        .map(_.toAbsolutePath)

//
//
//
//    //  val aggregateByProduit = parList.groupBy(transaction => (transaction.produit)).map({
//     //   case(k,v) => Transaction("",v.head.produit,v.map(_.qte).sum)
//     // })
//
//      sortedListOfProducts.foreach(println)
//     // aggregateByProduit.foreach(println)
//
//
//      //parList.foreach(println)
//      //transactionsMatrix.foreach(println)
//
//
//

     printToFile(new File("top_100_vente_GLOBAL_20170514x.data")) {
       p: PrintWriter => sortedListOfTopProducts.foreach(
          p.println
              )}

/*      val outputFile = "transactions_output.txt"
      val writerBufferOutupt = new BufferedWriter(new FileWriter(outputFile))

      sortedListOfProducts.foreach(writerBufferOutupt.write)
      writerBufferOutupt.close()*/


      transactionsBuffer.close()






      //*********************************************
      // **************** FIN ***********************
      //*********************************************

      val mb = 1024*1024
      val runtime = Runtime.getRuntime

      logger.info("Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
      logger.info("Free Memory:  " + runtime.freeMemory / mb)
      logger.info("Total Memory: " + runtime.totalMemory / mb)
      logger.info("Max Memory:   " + runtime.maxMemory / mb)

      // fin d'exécution du script
      val endTime = System.nanoTime()
      // calcul du temps total d'exécution
      val totalExecutionTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)
      logger.info(s" Execution time in Milliseconds: $totalExecutionTime")
    }


  }

