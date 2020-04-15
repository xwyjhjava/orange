package com.xiaoi.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Dataset, Row}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat


class BaseFun extends Serializable {


  // onehot编码
  def oneHotEncode(processedSet: Dataset[Row],
                   stringColumns: Array[String]) ={

    // String => IndexDouble
    val index_transformers = stringColumns.map(col => new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}Index"))

    //Index => SparseVector
    val index_pipeline = new Pipeline().setStages(index_transformers)
    val index_model = index_pipeline.fit(processedSet)
    val df_indexed = index_model.transform(processedSet)

    val indexColumns = df_indexed.columns.filter(x => x.contains("Index"))
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(indexColumns)
      .setOutputCols(stringColumns.map(name => s"${name}Vec"))
      .setDropLast(false)

    val encoded = encoder.fit(df_indexed).transform(df_indexed)
    encoded
  }


  //label encoding
  def labelEncode(df: Dataset[Row], cols: Array[String]): Dataset[Row] ={
    val indexers = cols.map(col => {
      new StringIndexer().setInputCol(col)
        .setOutputCol(col + "_index")
    })
    new Pipeline().setStages(indexers)
      .fit(df)
      .transform(df)
      .drop(cols: _*)
  }

  //分类器
  def train(dataset: Dataset[Row]): Unit ={

    val columns = dataset.columns
    val Array(training, test) = dataset.randomSplit(Array(0.8, 0.2))
    val assembler = new VectorAssembler()
      .setInputCols(columns)
      .setOutputCol("features")
    val labelIndexer = new StringIndexer()
      .setInputCol("classes")
      .setOutputCol("classesIndex")
      .fit(training)
    val booster = new XGBoostClassifier(
      Map("eta" -> 0.1f,
        "max_depth" -> 2,
        "objective" -> "binary:logistic",  //二分类
        "num_class" -> 2,       //类别个数
        "num_round" -> 100,
        "num_workers" -> 2
      )
    )
    booster.setFeaturesCol("features")
    booster.setLabelCol("classesIndex")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("realLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, labelIndexer, booster, labelConverter))
    val model = pipeline.fit(training)

    val prediction = model.transform(test)
    prediction.show(false)
  }



  val now = DateTime.now()
  //和现在相距天数
  def daysBetweenNow(target_date: String, pattern: String = "YYYY-MM-dd"): Int ={
    val fmtDate = DateTimeFormat.forPattern(pattern)
    val interval = Days.daysBetween(fmtDate.parseDateTime(target_date), now).getDays
    interval
  }

  //获得当前日之前某一天的时间
  def getDateBefore(month: Int, auto: String = "auto"): String = {
    if (auto == "auto") {
      now.minusMonths(month).toString("YYYY-MM-dd")
    } else {
      "1970-01-01"
    }
  }

}
