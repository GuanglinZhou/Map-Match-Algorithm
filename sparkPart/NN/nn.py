from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import MultilayerPerceptronClassifier

spark = SparkSession.builder.appName("test").getOrCreate()
sc = spark.sparkContext
index = 40664

trainData = sc.textFile('hdfs://master:9000//fcd/split/train/397-290_trainDataSplit/trainData_{}.csv'.format(index))
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
labelDF.write.save('hdfs://master:9000//test/labelIndexer_{}'.format(index), format='parquet', mode='append')

# df = spark.read.format('parquet').load('hdfs://master:9000//sparkExperiment/labelIndexer/labelIndexer_60438')
inputNode = len(columnName)-1
outputNode = len(label)
layers = [inputNode, 5, 4, outputNode]
trainer = MultilayerPerceptronClassifier(featuresCol="features", labelCol="label", maxIter=100, layers=layers,blockSize=128, seed=1234)
trainData = trainData.select("features", "indexedLabel").selectExpr("features as features", "indexedLabel as label")
model = trainer.fit(trainData)
test = sc.textFile('hdfs://master:9000//fcd/split/test/397-290_testDataSplit/testData_{}.csv'.format(index))
test = test.map(lambda line: line.split(','))
columnName = test.take(1)[0]
test = test.filter(lambda row: row != columnName).toDF(columnName)
test = test.rdd.map(lambda x: (Vectors.dense(x[0:-1]), x[-1])).toDF(["features", "label"])
model.save('hdfs://master:9000//test/model_{}'.format(index))
pred = model.transform(test)

sc.stop()
