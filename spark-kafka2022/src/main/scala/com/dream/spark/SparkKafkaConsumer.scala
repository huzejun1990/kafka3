package com.dream.spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author : huzejun
 * @Date: 2022/4/21-23:26
 */
object SparkKafkaConsumer {

  def main(args: Array[String]): Unit = {

    // 1 初始化上下文环境
//    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka2022")
val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka2022")

    val ssc = new StreamingContext(conf, Seconds(3))
//val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka2022")
//    val ssc = new StreamingContext(conf, Seconds(3))

    // 2 消费数据
    val kafkapara = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"master:9092,slave1:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG->"test"
    )
/*val kafkapara  = Map[String,Object](
  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"master:9092,slave1:9092",
  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
  ConsumerConfig.GROUP_ID_CONFIG->"test"
)*/
//    val KafkaDStream = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Set("first"),kafkapara)),
val KafkaDSteam = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("first"), kafkapara))

    // key "" value ""

    val valueDStream = KafkaDSteam.map(record => record.value())
    valueDStream.print()
/*
    val valueDStream = KafkaDStream.map(record => record.value())

    valueDStream.print()
*/

    // 3 执行代码 并阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}


