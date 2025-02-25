from pyspark.sql import SparkSession
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline

from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
spark=SparkSession.builder.appName("Tharun").master("yarn").getOrCreate()
# Create a DataFrame
df=spark.read.option("header",True).csv('s3://dataengineerp/input/train.csv')
test=spark.read.option("header",True).csv('s3://dataengineerp/input/test.csv')
df.groupby("quality").count().show()
from pyspark.sql.functions import isnull, when, count, col
df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()
df=df.drop("Id")
from pyspark.sql.functions import col
dataset = df.select(col('fixed acidity').cast('float'),
                         col('volatile acidity').cast('float'),
                         col('citric acid').cast("float"),
                         col('residual sugar').cast('float'),
                         col('chlorides').cast('float'),
                         col('free sulfur dioxide').cast('float'),
                         col("total sulfur dioxide").cast('float'),
                         col('density').cast('float'),
                         col('pH').cast('float'),
                         col('sulphates').cast('float'),
                         col('alcohol').cast('float'),col("quality").cast("float")
                        
                        )
dataset.show()
required_features = ['fixed acidity',
                 'volatile acidity',
                     'citric acid',
                 'residual sugar',
                     'chlorides',
                 'free sulfur dioxide',
                 'total sulfur dioxide',
                 'density',
             'pH',
             'sulphates',
             'alcohol'
 #  Vector Assembler                  ]
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=required_features, outputCol='features')
data = assembler.transform(dataset)
splits = data.randomSplit([0.7, 0.3])
train = splits[0]
test = splits[1].withColumnRenamed("label", "trueLabel")
train_rows = train.count()
test_rows = test.count()
print("Training Rows:", train_rows, " Testing Rows:", test_rows)
train.select("features").show(truncate=False)
lr = LogisticRegression(labelCol="quality",featuresCol="features",maxIter=10,regParam=0.3)
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(labelCol='quality', 
                            featuresCol='features',
                            maxDepth=5)
model = rf.fit(train)
predictions = model.transform(train)
predict = model.transform(test)
predict.select("prediction").show()
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(
    labelCol='quality', 
    predictionCol='prediction', 
    metricName='accuracy')
accuracy = evaluator.evaluate(predictions)
print('Train Accuracy = ', accuracy)
accuracy = evaluator.evaluate(predict)
print('Test Accuracy = ', accuracy)
test_main=test_main.drop("id")
from pyspark.sql.functions import col
dat = test.select(col('fixed acidity').cast('float'),
                         col('volatile acidity').cast('float'),
                         col('citric acid').cast("float"),
                         col('residual sugar').cast('float'),
                         col('chlorides').cast('float'),
                         col('free sulfur dioxide').cast('float'),
                         col("total sulfur dioxide").cast('float'),
                         col('density').cast('float'),
                         col('pH').cast('float'),
                         col('sulphates').cast('float'),
                         col('alcohol').cast('float')
                        
                        )
dat.show()
assembler = VectorAssembler(inputCols=required_features, outputCol='features')
test_data = assembler.transform(dat)
test_data.show()

pred=model.transform(test_data)
pred.count()
pred1=pred.select(col('prediction').cast('integer'))
pred1.write.mode("overwrite").csv("s3://dataengineerp/output/")

   
