package ru.itclover.streammachine.io.output


trait OutputConfig {

}

case class JDBCConfig(jdbcUrl: String,
                      sinkTable: String,
                      sinkColumnsNames: List[Symbol],
                      driverName: String,
                      userName: Option[String] = None,
                      password: Option[String] = None,
                      batchInterval: Option[Int] = None
                     ) extends OutputConfig