from pyspark.sql import functions as F

df_chi = spark.read.csv(path='s3://dan-data-for-exercise/Food_Inspections.csv', header='true')
df_nyc = spark.read.csv(path='s3://dan-data-for-exercise/DOHMH_New_York_City_Restaurant_Inspection_Results.csv', header='true')

df_chi = df_chi.select('DBA Name', 'Zip', 'City')
df_nyc = df_nyc.select('DBA', 'ZIPCODE').withColumn('CITY', F.lit('NYC'))

df = df_nyc.union(df_chi)

df.write.parquet(path='s3://dan-data-for-exercise/output', mode='overwrite')
