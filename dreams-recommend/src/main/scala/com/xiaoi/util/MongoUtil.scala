package com.xiaoi.util

import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import com.mongodb.MongoClient

/**
 * @Package com.xiaoi.util
 * @author ming
 * @date 2020/3/24 16:16
 * @version V1.0
 * @description mongo 增删改查的 util
 */
object MongoUtil {



  def mongoDBConn(mongoUri: String,
                  dbName: String,
                  collectName: String) = {
    import com.mongodb.MongoClientURI
    val connectionString = new MongoClientURI(mongoUri)
    val mongoClient = new MongoClient(connectionString)
    val database = mongoClient.getDatabase(dbName)
    val collection = database.getCollection(collectName)
    (mongoClient, collection)

  }





  def sparkWriteCol(docRDD: RDD[Document]
//                    mongoUri: String, colName:String
                    ) ={
    val writeConfig = WriteConfig(Map(
      "uri" -> "mongodb://xiaoi:xiaoi@127.0.0.1:27017/recommend",
      "collection" -> "itemLabel"
    ))
    MongoSpark.save(docRDD, writeConfig)

  }






  def main(args: Array[String]): Unit = {


    val mongoDBName = "recommend"
    var mongoColName = "itemLabel"
    val mongoUser = "xiaoi"
    val mongoPwd = "xiaoi"
    val mongoUriStr = s"mongodb://${mongoUser}:${mongoPwd}@127.0.0.1:27017/${mongoDBName}"
    val (client, collect) = mongoDBConn(mongoUriStr, mongoDBName, mongoColName)


    val doc = new Document()
    doc.append("labels", "畅销")
    collect.updateMany(new Document(), new Document().append("$push", doc))

  }


}
