#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-17 10:39:25
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$


'''

使用RF训练hdfs://master:9000//trainDataDir/trainData_xxx.csv文件

'''

# from __future__ import print_function

from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import MultilayerPerceptronClassifier

# import random

if __name__ == '__main__':
    spark = SparkSession.builder.appName("generateNNModel").getOrCreate()
    sc = spark.sparkContext

    ilist = [ 69731, 69732, 69736, 69742, 69743,
             69744, 69745, 69751, 69752, 69753, 69755, 69757, 70005, 70006, 70008, 70012, 70018, 70030, 70031, 70032,
             70033, 70034, 70042, 70043, 70044, 70045, 70046, 70048, 70049, 70295, 70296, 70297, 70298, 70299, 70300,
             70302, 70303, 70309, 70317, 70318, 70319, 70320, 70324, 70333, 70334, 70336, 70566, 70585, 70586, 70587,
             70588, 70590, 70591, 70592, 70593, 70606, 70607, 70610, 70623, 70624, 70625, 70626, 70855, 70856, 70875,
             70877, 70878, 70879, 70880, 70881, 70882, 70883, 70887, 70901, 70910, 70915, 70916, 71144, 71145, 71146,
             71165, 71166, 71168, 71169, 71170, 71171, 71172, 71202, 71206, 71435, 71436, 71455, 71467, 71485, 71724,
             71744, 71745, 71746, 71747, 71757, 71758, 71771, 71772, 71775, 72012, 72013, 72014, 72015, 72036, 72037,
             72038, 72039, 72060, 72061, 72062, 72325, 72326, 72327, 72328, 72329, 72334, 72348, 72349, 72350, 72591,
             72616, 72617, 72618, 72624, 72625, 72884, 72907, 72908, 72909, 72913, 72916, 72917, 73182, 73194, 73195,
             73197, 73203, 73205, 73472, 73485, 73486, 73487, 73489, 73491, 73492, 73494, 73775, 73776, 73781, 73782,
             73783, 73784, 74061, 74062, 74065, 74066, 74070, 74071, 74072, 74073, 74091, 74351, 74352, 74353, 74354,
             74355, 74356, 74359, 74361, 74362, 74381, 74641, 74642, 74643, 74644, 74645, 74646, 74649, 74650, 74651,
             74652, 74922, 74936, 74940, 75226, 75229, 75230, 75520, 75817, 76384, 76385, 76391, 76397, 76402, 76687,
             76691, 76692, 76962, 77251, 77252, 77255, 77256, 77540, 77541, 77542, 77828, 77829, 77830, 77831, 77832,
             78118, 78119, 78122, 78409, 78410, 78411, 78412, 79862, 80152]
    # indexRange = range(64832, 64838)
    # ilist = random.sample(ilist, 3)
    # ilist = [38116]
    for index in ilist:
        trainData = sc.textFile(
            'hdfs://master:9000//fcd/split/train/397-290_trainDataSplit/trainData_{}.csv'.format(index))
        trainData = trainData.map(lambda line: line.split(','))
        # columnName = trainData.first()
        columnName = trainData.take(1)[0]
        trainData = trainData.filter(lambda row: row != columnName).toDF(columnName)
        trainData = trainData.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
        labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(trainData)
        trainData = labelIndexer.transform(trainData)
        label = labelIndexer.labels
        labelDict = {}
        for i in range(len(label)):
            labelDict[label[i]] = i
        labelValIndex = list(labelDict.items())
        labelRdd = sc.parallelize(labelValIndex)
        labelDF = spark.createDataFrame(labelRdd, ['secID', 'index'])
        labelDF.write.save('hdfs://master:9000//fcd/split/NN/labelIndexer/labelIndexer_{}'.format(index),
                           format='parquet', mode='append')

        # df = spark.read.format('parquet').load('hdfs://master:9000//sparkExperiment/labelIndexer/labelIndexer_60438')
        inputNode = len(columnName) - 1
        outputNode = len(label)
        layers = [inputNode, 5, 4, outputNode]
        trainer = MultilayerPerceptronClassifier(featuresCol="features", labelCol="label", maxIter=100, layers=layers,
                                                 blockSize=128, seed=1234)
        trainData = trainData.select("features", "indexedLabel").selectExpr("features as features",
                                                                            "indexedLabel as label")
        model = trainer.fit(trainData)

        model.save('hdfs://master:9000//fcd/split/NN/serialModel/model_{}'.format(index))
    sc.stop()



spark = SparkSession.builder.appName("generateNNModel").getOrCreate()
sc = spark.sparkContext
index=40664
trainData = sc.textFile(
    'hdfs://master:9000//fcd/split/train/397-290_trainDataSplit/trainData_{}.csv'.format(index))
trainData = trainData.map(lambda line: line.split(','))
columnName = trainData.take(1)[0]
trainData = trainData.filter(lambda row: row != columnName).toDF(columnName)
trainData = trainData.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(trainData)
trainData = labelIndexer.transform(trainData)
label = labelIndexer.labels
labelDict = {}
for i in range(len(label)):
    labelDict[label[i]] = i
labelValIndex = list(labelDict.items())
labelRdd = sc.parallelize(labelValIndex)
labelDF = spark.createDataFrame(labelRdd, ['secID', 'index'])
labelDF.write.save('hdfs://master:9000//fcd/split/NN/labelIndexer/labelIndexer_{}'.format(index),
                   format='parquet', mode='append')

# df = spark.read.format('parquet').load('hdfs://master:9000//sparkExperiment/labelIndexer/labelIndexer_60438')
inputNode = len(columnName) - 1
outputNode = len(label)
layers = [inputNode, 5, 4, outputNode]
trainer = MultilayerPerceptronClassifier(featuresCol="features", labelCol="label", maxIter=100, layers=layers,
                                         blockSize=128, seed=1234)
trainData = trainData.select("features", "indexedLabel").selectExpr("features as features",
                                                                    "indexedLabel as label")
model = trainer.fit(trainData)

model.save('hdfs://master:9000//fcd/split/NN/serialModel/model_{}'.format(index))
sc.stop()