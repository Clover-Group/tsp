//package ru.itclover.tsp.http
//
//import java.util
//import java.net.URLDecoder
//import akka.actor.ActorSystem
//import akka.http.scaladsl.Http
//import akka.stream.ActorMaterializer
//import com.typesafe.config.ConfigFactory
//import com.typesafe.scalalogging.Logger
//import org.apache.flink.api.common.ExecutionConfig
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.io.CollectionInputFormat
//import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
//import org.apache.flink.streaming.api.functions.source.FromElementsFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.types.Row
//import org.influxdb.dto.QueryResult
//import scala.concurrent.duration._
//import scala.concurrent.{Await, ExecutionContextExecutor}
//import scala.io.StdIn
//import collection.JavaConversions._
//import ru.itclover.tsp.utils.DataStreamOps.DataStreamOps
//import scala.collection.immutable
//
//object TestLauncher extends App with HttpService {
//  private val configs = ConfigFactory.load()
//  override val isDebug: Boolean = configs.getBoolean("general.is-debug")
//  private val isListenStdIn = configs.getBoolean("general.is-follow-input")
//  private val log = Logger("Launcher")
//
//  implicit val system: ActorSystem = ActorSystem("TSP-test-system")
//  implicit val materializer: ActorMaterializer = ActorMaterializer()
//  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//
//  val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
//  // streamEnvironment.setMaxParallelism(configs.getInt("flink.max-parallelism"))
//
//
//  val rawJarPath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
//  val jarPath = URLDecoder.decode(rawJarPath, "UTF-8")
//  println(jarPath)
//
//  /*implicit val intTypeInfo = TypeInformation.of(classOf[Int])
//
//  val qr1 = new QueryResult.Result()
//  val qr1s = new QueryResult.Series()
//  qr1s.setColumns(List("1"))
//
//  implicit def i2I(x: Int) = java.lang.Integer.valueOf(x)
//
//  val jl = asJavaCollection(List(i2I(1), i2I(2), i2I(3))).toList
//  val il = new util.ArrayList[AnyRef]()
//  il.add(i2I(1)); il.add(i2I(2)); il.add(i2I(3));
//  val l = new util.ArrayList[util.List[AnyRef]]()
//  l.add(il)
//
//  qr1s.setValues(l)
//  qr1s.setTags(Map("1" -> "1"))
//  qr1.setSeries(List(qr1s))
//
//  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
//  implicit val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
//
//  val c1 = asJavaCollection(List(qr1))
//  val functionA = new CollectionInputFormat[QueryResult.Result](c1, queryResultTypeInfo.createSerializer(new ExecutionConfig()))
//
//  // val c2 = asJavaCollection(List(4, 5))
//  // val functionB = new FromElementsFunction[Int](intTypeInfo.createSerializer(new ExecutionConfig()), c2)
//
//  /*val src = new EndingInputFormatCnsl(functionA, List())
//
//  streamEnvironment.setParallelism(1)
//  val s = streamEnvironment.createInput(src)
//
//  s.foreach(x => println(s"xxx = $x"))*/
//  // s1.foreach(x => println(s"yyy = $x"))
//
//  /*val source = streamEnvironment.fromElements(1.to(50000000) :_*)
//  val multByTwo = source.map(_ * 2)
//  val addTen = source.map(_ + 10)
//  multByTwo.union(addTen).addSink(println(_))
//
//   val fieldClasses: List[Class[_]] = List(classOf[String], classOf[Double])
//
//  val r: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
//
//  val typeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(r)
//
//  val s = streamEnvironment.createInput(InfluxDBInputFormat.create()
//    .url("http://localhost:8086")
//    .username("clover")
//    .password("g29s7qkn")
//    .database("NOAA_water_database")
//    .query("select time, water_level from h2o_feet limit 100")
//    .and().buildIt())(typeInfo)
//    .flatMap(queryResult => {
//      for {
//        series <- queryResult.getSeries
//        tags = if (series.getTags != null) series.getTags else new util.HashMap[String, String]()
//        valueSet <- series.getValues
//      } yield {
//        val row = new Row(tags.size() + valueSet.size())
//        tags.toSeq.sortBy(_._1).zipWithIndex.foreach { case ((_, tag), ind) => row.setField(ind, tag) }
//        valueSet.zipWithIndex.foreach { case (value, ind) => row.setField(ind, value) }
//        row
//      }
//    })(new RowTypeInfo(fieldClasses.map(TypeInformation.of(_)) :_*))
//
//  s.print()*/
//
//  streamEnvironment.execute()*/
//}
