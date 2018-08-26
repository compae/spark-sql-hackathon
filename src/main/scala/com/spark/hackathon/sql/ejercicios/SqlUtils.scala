package com.spark.hackathon.sql.ejercicios

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SqlUtils {

  val sparkConf: SparkConf

  val sparkSession: SparkSession

  /** Leer fichero csv de tiendas, requisitos:
    *
    * Modificar el tipo del campo codProvincia para que sea un string
    */
  def leerFicheroTiendas: DataFrame

  /** Leer fichero csv de provincias, requisitos:
    *
    * Añadir un campo adicional con el nombre codProvincia de tipo string a partir del campo postal_code
    */
  def leerFicheroProvincias: DataFrame

  /** Leer fichero json de tickets */
  def leerFicheroTickets: DataFrame

  /** Registrar como tablas temporales los ficheros leídos */
  def registrarTablas(dataFramesARegistrar: Map[String, DataFrame]): Unit

  /** Guardar en formato parquet un dataFrame */
  def guardarEnParquet(dataFrame: DataFrame, path: String): Unit

  /** Total de ventas de todos los tickets por tienda enriqueciendo con los datos de tiendas(Join tickets, stores) */
  def totalVentaPorTienda(tickets: DataFrame, tiendas: DataFrame): DataFrame

  /** Total de ventas de todos los tickets por codigo corto de provincia (Join tickets, stores, provincias) donde el total del ticket sea mayor que 1 euro */
  def totalVentaPorNombreProvincia(): DataFrame

  /** Top 20 de registros */
  def top20RegistrosJson(dataFrame: DataFrame): Seq[String]

}
