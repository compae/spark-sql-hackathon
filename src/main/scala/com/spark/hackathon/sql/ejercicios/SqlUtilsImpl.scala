package com.spark.hackathon.sql.ejercicios

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._

class SqlUtilsImpl extends SqlUtils {

  val sparkConf: SparkConf = {
    val configurationProps = ConfigFactory.load().getConfig("spark").entrySet()
      .map(prop => (s"spark.${prop.getKey}", prop.getValue.unwrapped().toString)).toSeq

    new SparkConf().setAll(configurationProps)
  }

  val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  override def leerFicheroTiendas: DataFrame = {
    sparkSession.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/home/jcgarcia/hackathon/tiendas.csv")
  }

  override def leerFicheroProvincias: DataFrame = {
    sparkSession.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/home/jcgarcia/hackathon/provincias.csv")
  }

  override def leerFicheroTickets: DataFrame = {
    sparkSession.read.json("/home/jcgarcia/hackathon/tickets.json")
  }

  override def registrarTablas(dataFramesARegistrar: Map[String, DataFrame]): Unit = {
    dataFramesARegistrar.foreach { case (tableName, dataFrame) =>
      dataFrame.createOrReplaceGlobalTempView(tableName)
    }
  }

  override def guardarEnParquet(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def totalVentaPorTienda(tickets: DataFrame, tiendas: DataFrame): DataFrame = ???

  override def totalVentaPorNombreProvincia(): DataFrame = ???

  override def top20RegistrosJson(dataFrame: DataFrame): Seq[String] = {
    dataFrame.toJSON.take(20)
  }

}
