package com.spark.hackathon.sql.explicacion

import akka.event.slf4j.SLF4JLogging
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._


/**
  * Repasar las funcionalidades básicas del SQL de Spark
  */
object SparkSqlMain extends App with SLF4JLogging {


  /** Creación de la session */

  val configurationProps  = ConfigFactory.load().getConfig("spark").entrySet()
    .map(prop => (s"spark.${prop.getKey}", prop.getValue.unwrapped().toString)).toSeq

  val sparkConf = new SparkConf().setAll(configurationProps)
  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._

  /** Creación de dataframes y tablas */

  println(s"Dataframe canciones ...")

  val canciones = sparkSession.read.json("/home/jcgarcia/hackathon/canciones.json")
  canciones.show()
  canciones.printSchema()

  canciones.createOrReplaceTempView("canciones")

  println(s"Dataframe paises ...")

  val paises = sparkSession.read
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/home/jcgarcia/hackathon/paises.csv")

  paises.show()
  paises.printSchema()

  paises.createOrReplaceTempView("paises")

  /** Tablas globales y multisesiones */

  println(s"Primera query sobre canciones ...")

  val session2 = sparkSession.newSession()
  val query = sparkSession.sql("select * from canciones")
  query.show()

  //ERROR: val query2 = session2.sql("select * from canciones")

  println(s"Primera query sobre tablas globales ...")

  canciones.createOrReplaceGlobalTempView("canciones2")
  val query3 = sparkSession.sql("select * from global_temp.canciones2")
  query3.show()
  val query4 = session2.sql("select * from global_temp.canciones2")
  query4.show()

  /** Dataframes, schemas, rdd, funciones, serializacion */

  println(s"Primer dataframe creado en base a un RDD ...")

  val schema = canciones.schema
  val rdd = canciones.rdd
  val canciones2 = sparkSession.createDataFrame(rdd, schema)
  canciones2.show()
  canciones2.printSchema()

  println(s"Primer dataframe creado en base a un RDD, modificando schema y datos ...")

  val selectRdd = rdd.map(row => Row(row.get(0)))
  val selectSchema = StructType(Seq(schema.get(0)))
  val selectDataFrame = sparkSession.createDataFrame(selectRdd, selectSchema)
  selectDataFrame.show()
  selectDataFrame.printSchema()

  println(s"Primer dataframe creado en base a un RDD, modificando schema y datos con la serializacion de clases ...")

  case class Album(album: String)
  val selectRddClass = rdd.map(row => Album(row.get(0).toString))
  val selectDataFrameClass = selectRddClass.toDF()
  selectDataFrameClass.show()
  selectDataFrameClass.printSchema()

  println(s"Nuevos dataframes usando las funciones y las DSL de Spark ...")

  val selectConDsl = canciones.select($"album" as "albumModificado")
  selectConDsl.show()
  selectConDsl.printSchema()

  val selectSinDsl = canciones.select("album")
  selectSinDsl.show()
  selectSinDsl.printSchema()

  val selectExpression = canciones.selectExpr("album AS albumModificado")
  selectExpression.show()
  selectExpression.printSchema()

  val selectByQuery = sparkSession.sql("select album AS albumModificado from canciones")
  selectByQuery.show()
  selectByQuery.printSchema()

  val filterConDsl = canciones.filter($"genre" === "Latin")
  filterConDsl.show()
  filterConDsl.printSchema()

  val groupByConDsl = canciones.groupBy($"genre").count()
  groupByConDsl.show()
  groupByConDsl.printSchema()

  println(s"Primer dataset sin implicitos, usando el encoder de clases ...")

  case class Canciones(album: String, artist: String, genre: String, song: String)
  val cancionesDataSet = canciones.as[Canciones]
  cancionesDataSet.show()
  cancionesDataSet.printSchema()

  /** Guardar datos en otros formatos */

  println(s"Guardar los datos en un nuevo fichero con otro formato ...")

  canciones.write
    .mode(SaveMode.Overwrite)
    .partitionBy("genre")
    .parquet("/home/jcgarcia/hackathon/canciones_parquet")

  val parquetCanciones = sparkSession.read
    .parquet("/home/jcgarcia/hackathon/canciones_parquet")
  parquetCanciones.show()
  parquetCanciones.printSchema()

}

