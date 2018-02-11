package carrefour.challenge.phenix.caseclass

case class Transaction(
                        txId: Long,
                        dateTime: String,
                        magasin: String,
                        produit: Long,
                        qte: Long
                      )

case class TransactionTripletCA(
                                 magasin: String,
                                 produit: Long,
                                 prix: Double)

case class TransactionTriplet(
                               magasin: String,
                              produit: Long,
                              qte: Long
                             )

case class TransactionTuple(
                             produit: Long,
                            qte: Long
                           )
