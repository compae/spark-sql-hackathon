package com.spark.hackathon.sql.explicacion

import akka.event.slf4j.SLF4JLogging
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._


/**
  * Repasar las funcionalidades básicas del SQL de Spark
  */
object SparkSqlMain extends App with SLF4JLogging {


  /** Creación de la session **/

  val configurationProps  = ConfigFactory.load().getConfig("spark").entrySet()
    .map(prop => (s"spark.${prop.getKey}", prop.getValue.unwrapped().toString)).toSeq

  val sparkConf = new SparkConf().setAll(configurationProps)
  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._

  val canciones = sparkSession.read.json("/home/jcgarcia/hackathon/canciones.json")
  canciones.show()
  canciones.printSchema()

  val paises = sparkSession.read
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/home/jcgarcia/hackathon/paises.csv")

  paises.show()
  paises.printSchema()



}

