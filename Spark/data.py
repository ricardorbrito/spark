
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



