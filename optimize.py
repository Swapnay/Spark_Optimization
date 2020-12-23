'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark import StorageLevel
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import col, count, month,broadcast


import os

# change to orderby of answer sp 9s


spark = SparkSession.builder.appName('Optimize II').getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions",20)
base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3])
answers_input_path = os.path.join('file:////'+ 'Users/syeruvala/Exercism/python/Spark_Optimization', 'data/answers')

questions_input_path = os.path.join('file:////'+'Users/syeruvala/Exercism/python/Spark_Optimization', 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()


'''
Answers aggregation

Here we : get number of answers per question per month
'''
answers_month = answersDF.withColumn('month', month('creation_date')).repartition(col('question_id'),col('month')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')
resultDF.orderBy('question_id', 'month').show()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''