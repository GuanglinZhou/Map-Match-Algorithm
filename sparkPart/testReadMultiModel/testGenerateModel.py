#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-20 10:39:25
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$


'''

使用iris.data.txt作为训练集，用random_forest_classifier_example.py训练得到多个模型

'''


from __future__ import print_function

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("generateModel").getOrCreate()

sc = spark.sparkContext
train1 = sc.textFile('hdfs://master:9000//testMultiModel/input/iris.data1.txt')
train1 = train1.map(lambda line: line.split(','))
train1 = train1.map(lambda line: Row(chang=line[0], kuan=line[1], gao=line[2], hou=line[3], label=line[4])).toDF()
train1 = train1.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(train1)
train1 = labelIndexer.transform(train1)
rf1 = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol='indexedLabel', featuresCol='features', seed=42)
model1 = rf1.fit(train1)
# # 用model1预测train2
# train2 = sc.textFile('hdfs://master:9000//testMultiModel/input/iris.data2.txt')
# train2 = train2.map(lambda line: line.split(','))
# train2 = train2.map(lambda line: Row(chang=line[0], kuan=line[1], gao=line[2], hou=line[3], label=line[4])).toDF()
# train2 = train2.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
# labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(train2)
# train2 = labelIndexer.transform(train2)
# predictions = model1.transform(train2)
# labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
#                                labels=labelIndexer.labels)
# predictions = labelConverter.transform(predictions)
# '''
# >>> predictions.show(2)
# +-----------------+-----------+------------+-------------+-------------+----------+---------------+
# |         features|      label|indexedLabel|rawPrediction|  probability|prediction| predictedLabel|
# +-----------------+-----------+------------+-------------+-------------+----------+---------------+
# |[5.0,1.4,0.2,3.6]|Iris-setosa|         0.0|[0.0,0.0,3.0]|[0.0,0.0,1.0]|       2.0|Iris-versicolor|
# |[4.9,1.5,0.1,3.1]|Iris-setosa|         0.0|[0.0,0.0,3.0]|[0.0,0.0,1.0]|       2.0|Iris-versicolor|
# +-----------------+-----------+------------+-------------+-------------+----------+---------------+
# only showing top 2 rows
# '''
# predictions = predictions.select('features', 'predictedLabel')
#
# '''
# >>> predictions.show(2)
# +-----------------+---------------+
# |         features| predictedLabel|
# +-----------------+---------------+
# |[5.0,1.4,0.2,3.6]|Iris-versicolor|
# |[4.9,1.5,0.1,3.1]|Iris-versicolor|
# +-----------------+---------------+
# only showing top 2 rows
#
# '''
# 用train2训练model2,并持久化到hdfs中
train2 = sc.textFile('hdfs://master:9000//testMultiModel/input/iris.data2.txt')
train2 = train2.map(lambda line: line.split(','))
train2 = train2.map(lambda line: Row(chang=line[0], kuan=line[1], gao=line[2], hou=line[3], label=line[4])).toDF()
train2 = train2.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(train2)

train2 = labelIndexer.transform(train2)
rf2 = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol='indexedLabel', featuresCol='features', seed=42)
model2 = rf2.fit(train2)

# 将模型持久化到hdfs中，并且重新读取模型，经测试，下列方法运行正确。
model1.save('hdfs://master:9000//testMultiModel/input/model1')
model2.save('hdfs://master:9000//testMultiModel/input/model2')

model1_load = RandomForestClassificationModel.load('hdfs://master:9000//testMultiModel/input/model1')

test = sc.textFile('hdfs://master:9000//testMultiModel/input/iris.test.txt')
test = test.map(lambda line: line.split(','))
test = test.map(lambda line: Row(chang=line[0], kuan=line[1], gao=line[2], hou=line[3])).toDF()
test = test.withColumn('label', lit(0))
test = test.rdd.map(lambda x: (Vectors.dense(x[:-1]), x[-1])).toDF(["features", "label"])
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(test)
test = labelIndexer.transform(test)


def transFunc(s):
    model1_load = RandomForestClassificationModel.load('hdfs://master:9000//testMultiModel/input/model1')
    pred = model1_load.transform(s)
    print(pred)
    return pred.select()


labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)
predictions = labelConverter.setHandleInvalid("skip").transform(predictions)

transFunc_udf = udf(transFunc)
test = test.withColumn('predLabel', transFunc_udf('features'))
predictions = model1.transform(test)
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)
predictions = labelConverter.transform(predictions)
spark.stop()
