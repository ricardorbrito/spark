
#api spark 

from pyspark.sql import SparkSession



spark = SparkSession.builder.getOrCreate()

spark.sql("""
            CREATE TEMPORARY VIEW vw_tablename
            USING org.apache.spark.sql.json 
            OPTIONS (path "/caminho do dataset/*.json") 

          """)

print(spark.catalog.listTables())


spark.sql("""SELECT *  FROM TABLENAME;""").show();

join_dataset = spark.sql(""" 
                         SELECT CAMPOS.....,
                           FROM TABLENAME1 INNE JOIN T1
                                TABLENAME2 T2 ON T1.ID = T2.ID;
                         """)


join_dataset.show()


#pandas developer
#arrow is an eginer in memory
builder = SparkSession.Builder.appName("APP")
builder = builder.config("spark.sql.excetution.arrow.pyspark.enabled","true")
builder.getOrCreate()
print(builder)
#panda on spark
import pyspark.pandas as ps #Use Spark's distributed computing executors.
#import pandas as ps #Do not use Spark's distributed computing executors

#read files

get_device = ps.read_json("/path of filies/*.json")

print(get_device)

get_device.info()

get_device.spark.explain(mode="formatted") 

