package com.atguigu.gmall1021.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.app.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topic = "ODS_BASE_LOG"
    val groupId = "dau_group"
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    //inputDstream.map(_.value()).print()

    //统计用户当日的首次访问 dau uv
    //  可以通过判断 日志page栏位是否有last_page_id来决定该页面位 -> 一次访问会话的首个页面
    //  也可以通过启动日志来判断 是否首次访问


    //转换操作 转换格式 转换成方便操作的jsonObj
    val logJsonDStream: DStream[JSONObject] = inputDstream.map {
      record =>
        val jsonString: String = record.value() //ConsumerRecord[String, String]将后面的String提取出来
        val logJsonObj: JSONObject = JSON.parseObject(jsonString)
        //把ts转换成日期和小时
        val ts: lang.Long = logJsonObj.getLong("ts")
        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHourString: String = simpleDateFormat.format(new Date(ts))
        val dt = dateHourString.split(" ")(0)
        val hr: String = dateHourString.split(" ")(1)
        //取到的日期再放回
        logJsonObj.put("dt",dt)
        logJsonObj.put("hr",hr)
        logJsonObj
    }

    //过滤操作  得到每次会话的第一个访问页面
    val firstPageJsonDStream: DStream[JSONObject] = logJsonDStream.filter {
      logJsonObj =>
        var isFirstPage = false //设置一个布尔变量 用于控制过滤
        //有page元素 但是page元素中没有last_page_id的要保留（返回true）其他的过滤掉（返回false）
        val pageJsonObj: JSONObject = logJsonObj.getJSONObject("page")
        if (pageJsonObj != null) {
          val lastPageId: String = pageJsonObj.getString("last_page_id")
          if (lastPageId == null) { //如果lastPageId为空 说明没有上一页 是首次访问 保留
            //如果不为空 ifFirstPage还是false 过滤掉
            isFirstPage = true
          }
        }
        isFirstPage
    }
    //firstPageJsonDStream.print(1000)
    //要把会话的首页 ->去重-> 当日的首次访问（日活）
    //如何去重：本质来说就是一种识别  识别每条日志的主体（mid）  对于当日来说是不是已经来过了
    //如何保存用户访问清单：redis mySQL hbase es hive hdfs kafka
    // updateStateByKey  checkpoint 弊端：不好管理 容易非常臃肿  [mapStateByKey experiment]
    //如何把用户访问清单保存在redis中
    // type？五大数据类型 set  key？dau：2021-02-27  value？mid
    // 读写api？ sadd 即做了判断 又做了写入 看sadd后的返回结果 成功插入 返回1 重复插入 返回0
    // 过期时间？ 24H
    val dauJsonDstream: DStream[JSONObject] = firstPageJsonDStream.filter {
      logJsonObj =>
        val jedis: Jedis = new Jedis("hadoop105", 6379)//建立连接
        val dauKey = "dau:" + logJsonObj.getString("dt")//设定key 每天一个清单 所以每个日期有一个key
        //获取mid 先获取JSON下的common 才能获取mid
        val mid: String = logJsonObj.getJSONObject("common").getString("mid")//从json中取得mid
        val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid) //sadd后会返回值 1或0 1表示新数据 0表示清单中有 舍弃
        if (isFirstVisit == 1L) { //因为isFirstVisit是long所以1L
          true
        } else {
          false
        }
    }

    dauJsonDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}
