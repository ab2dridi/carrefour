package carrefour.challenge.phenix.utils

import scopt.OptionParser

object CmdLineParser {
  case class Config(
                   action: String = "run",
                   fileDirectory: String = ""
                   )

    val parser: OptionParser[Config] = new scopt.OptionParser[Config]("scopt") {
      opt[String]('d', "file-directory").action(
        (x, c) => c.copy(fileDirectory = x) ).text("File Directory")
        .required()

      cmd("run").action((_, c) => c.copy(action = "run"))
        .text("Run the process")
    }
}
