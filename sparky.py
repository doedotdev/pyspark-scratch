per_date = df.withColumn('simple_date',
                         concat(
                             year('dttm').cast(StringType()),
                             lit("/"),
                             month('dttm').cast(StringType()),
                             it("/"),
                             dayofmonth('dttm').cast(StringType())))\
    .groupBy('simple_date')\
    .count()\
    .orderBy('count', ascending=False)\
    .na.drop()
