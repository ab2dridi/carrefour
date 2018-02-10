package carrefour.challenge.phenix.utilities

import java.text.SimpleDateFormat
import java.util.Calendar

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
    try { op(p) } finally { p.close() }
  }
}
