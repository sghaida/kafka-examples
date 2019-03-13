package com.sghaida.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


object JsonUtil {

  val mapper = new ObjectMapper() with ScalaObjectMapper

  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JodaModule())
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE,true)
  mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false)
  mapper.configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS,false)

  def toJson(value: Map[Symbol, Any]): String = toJson(value map { case (k,v) => k.name -> v})

  def toJson(value: Any): String = mapper.writeValueAsString(value)

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = mapper.readValue[T](json)

  def toMap[V](json:String)(implicit m: Manifest[V]): Map[String, V] = fromJson[Map[String,V]](json)

}
