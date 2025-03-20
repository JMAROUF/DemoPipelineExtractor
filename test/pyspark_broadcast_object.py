from pyspark.sql import SparkSession

if __name__=='__main__':
    spark=SparkSession.builder.appName("TestBroadcastObject").master("local[*]").getOrCreate()
    data=[("James","USA"),("Smith","CA")]
    columns=["Name","Country"]
    df=spark.createDataFrame(data,columns)
    data_broadcast=spark.sparkContext.broadcast({"James":100,"Smith":50})
    def add_salary(row):
        Salary=data_broadcast.value.get(row["Name"],0)
        return (row["Name"],row["Country"],Salary)
    result=df.rdd.map(add_salary).toDF(columns)
    result.show()
    spark.stop()