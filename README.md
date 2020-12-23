Spark Optimization:

** 1:** Standalone spark with:
                Master: 15 GB memory and 12 Cores
                Worker: 1 GB per executor 6 cores per executor.

** 2:** Spark submit command used.

```bin/spark-submit  --master spark://Host:7077 optimize.py```

**1. Resource Tuning ** [Here](https://blog.cloudera.com/how-to-tune-your-apache-spark-jobs-part-2/)
    * Since spark was running in stand alone mode, increasing the number of executors had negative effect on performance.
    * having 2 worker nodes with 1 executor and 6 core took 14 sec to complete the job where as having single worker node with 2 executors took 13 seconds to complete
    * When number of executors were increased to 10 with single core, job took more than 30 seconds to complete
    * Best time of 12 Seconds was achieved with below configuration on single worker machine
    ``` --num-executors 1 --executor-cores 12  --executor-memory 1GB ```

** Data frame Caching **
   * DataFrame caching using cache()/persist() had no effect at all.Before and after execution times were same.
   ``` answers_month.cache() ```

** Repartitioning **
   * Repartitioning answersDF had 2 seconds gain when default spark configuration was use.
   ``` answersDF.withColumn('month', month('creation_date')).repartition(col('question_id'),col('month')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))```

** Broadcast and join order **
   * Using either Broadcast or changing join order of questionsDF and answers_month had no effect on time either
   * From [Here](https://spark.apache.org/docs/latest/configuration.html) i see that by default under 10MB broadcast is on.

** Limiting shuffle partitions **
   * I see that by default number of partitions are 200.That will cause lot of shuffle.Using  ```spark.sql.shuffle.partitions``` config option number of shuffle partitions can be controlled.
   * setting this to less than 20 had 2 second performance gain.

