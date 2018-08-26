package com.spark.hackathon.sql.ejercicios

import akka.event.slf4j.SLF4JLogging


object SparkSql extends App with SLF4JLogging {


  /** Crear una implementación de SqlUtils, requisitos:
    *
    * Dar un nombre a la app de Spark
    * Asignarle un master
    * Implementar las funciones sobre DataFrames/DataSet[Row]
    *
    */

  val sqlUtils = new SqlUtilsImpl
  val provincias = sqlUtils.leerFicheroProvincias
  val tickets = sqlUtils.leerFicheroTickets
  val tiendas = sqlUtils.leerFicheroTiendas

  sqlUtils.registrarTablas(Map(
    "tiendas" -> tiendas,
    "tickets" -> tickets,
    "provincias" -> provincias
  ))

  sqlUtils.guardarEnParquet(tickets, "/home/jcgarcia/hackathon/tickets")

  val totalVentaPorTienda = sqlUtils.totalVentaPorTienda(tickets, tiendas)
  val topTotalVentaPorTienda = sqlUtils.top20RegistrosJson(totalVentaPorTienda)

  log.error(s"Top ventas por tienda: \n\t${topTotalVentaPorTienda.mkString(",")}")

  val totalVentaPorNombreProvincia = sqlUtils.totalVentaPorNombreProvincia()
  val topTotalVentaPorNombreProvincia = sqlUtils.top20RegistrosJson(totalVentaPorTienda)

  log.error(s"Top ventas por provincia: \n\t${topTotalVentaPorNombreProvincia.mkString(",")}")

}

