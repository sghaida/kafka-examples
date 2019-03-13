package com.sghaida.models

import com.sghaida.utils.JsonUtil
import org.joda.time.DateTime
import scala.util.Random

case class Transaction (name: String, amount: Int, time: DateTime)

object Transaction{

  val r: Random = Random
  r.setSeed(42L)

  private def apply(name: String): Transaction = new Transaction(
    name,
    r.nextInt(1000),
    DateTime.now().minusMinutes(r.nextInt(60)).minusHours(r.nextInt(24))
  )

  def generateTransactions(name: String, count: Int): IndexedSeq[String] =
    for (_ <- 0 until count) yield toJson(apply(name))

  private def toJson(t: Transaction): String = JsonUtil.toJson(t)
  def fromJson(t: String): Transaction = JsonUtil.fromJson[Transaction](t)

}