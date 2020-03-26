package com.xiaoi.util

import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import com.mongodb.{BasicDBObject, MongoClient}
import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor}

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
    val collection: MongoCollection[Document] = database.getCollection(collectName)
    (mongoClient, collection)

  }














  /**
   *
   * @param docRDD
   * @description spark的方式写mongo
   *
   */

  def sparkWriteCol(docRDD: RDD[Document]
//                    mongoUri: String, colName:String
                    ) ={
    val writeConfig = WriteConfig(Map(
      "uri" -> "mongodb://xiaoi:xiaoi@127.0.0.1:27017/recommend",
      "collection" -> "itemLabel"
    ))
    MongoSpark.save(docRDD, writeConfig)

  }


  /**
   *
   * @param id        主键
   * @param collection  mongo collection对象
   * @param oldKey     更新的旧key
   * @param oldVal     更新的旧val
   * @param newVal     更新的新val
   * @return
   */
  def updateOneById(id: String,
                    collection: MongoCollection[Document],
                    oldKey: String,
                    oldVal: String,
                    newVal: String): Boolean ={

    val queryDoc = new Document()
    queryDoc.put("labels."+ oldKey, oldVal)
    val updateDoc = new Document("labels.$."+ oldKey, newVal)
    val modifiedCountAvailable: Boolean = collection.updateOne(queryDoc, new Document("$set", updateDoc)).isModifiedCountAvailable
    modifiedCountAvailable
  }






  def main(args: Array[String]): Unit = {


    val mongoDBName = "recommend"
    val mongoColName = "itemLabel"
    val mongoUser = "xiaoi"
    val mongoPwd = "xiaoi"
    val mongoUriStr = s"mongodb://${mongoUser}:${mongoPwd}@127.0.0.1:27017/${mongoDBName}"
    val (client, collect) = mongoDBConn(mongoUriStr, mongoDBName, mongoColName)




//    val doc = new Document()
//    doc.append("labels", "畅销")
//    collect.updateMany(new Document(), new Document().append("$push", doc))


    val query = new Document()
    query.put("item_id", 34226016)


    // 查询到指定item_id的数据
    val json: String = collect.find(query).first().toJson
    println(json)


    val doc: Document = Document.parse(json)
    val itemId: AnyRef = doc.getInteger("item_id")
    println("item_id ==> " + itemId)

    val labels: AnyRef = doc.getString("labels")
    println("labels ==> " + labels)


    println("=============  更新该document")

    val updateDocument: Document = new Document("天气畅销", "雨天畅销")
//    val updateDocument2: Document = new Document("畅销度", "极畅销")

    collect.updateOne(query, new Document("$push", new Document("labels", updateDocument)))
//    collect.updateOne(query, new Document("$push", new Document("labels_5", updateDocument2)))

    println("===============更新成功")

//    val queryObj = new BasicDBObject()
//    queryObj.put("item_id", 34226016)



    println("--------------修改其中的一个值")
    //修改值


//    query.put("labels_5.天气畅销", "雨天畅销")
//    val updateDoc = new Document("labels_5.$.天气畅销", "晴天")


//    val modifiedCountAvailable: Boolean = collect.updateOne(query, new Document("$set", updateDoc)).isModifiedCountAvailable
//
//    println(modifiedCountAvailable)

    println("--------------修改成功")


//    val iterator: MongoCursor[Document] = collect.find().limit(10).iterator()
//    var res = List[String]()
//    while (iterator.hasNext) {
//      val item = iterator.next().values().toArray().mkString("|")
//      res.::=(item)
//    }
//
//    println(res.size)


//    val doc: String = collect.find(query).first().toJson()

//    println(doc)

//    val labelValue: AnyRef = doc.get("item_id")

//    val labelValue: String = doc.getString("labels")

//    println(labelValue)



//    collect.findOneAndUpdate()




  }



}
