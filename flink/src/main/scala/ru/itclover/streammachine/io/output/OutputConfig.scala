package ru.itclover.streammachine.io.output


trait OutputConfig


case class JDBCOutputConfig(jdbcUrl: String,
                            sinkSchema: JDBCSegmentsSink,
                            driverName: String,
                            userName: Option[String] = None,
                            password: Option[String] = None,
                            batchInterval: Option[Int] = None
                           ) extends OutputConfig