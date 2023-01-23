#ana 1
df.where(df.PRSN_GNDR_ID=="MALE").groupBy(df.CRASH_ID,df.PRSN_GNDR_ID).count().show()
#ana 2
df2.where(df2.VEH_BODY_STYL_ID.isin('MOTORCYCLE','POLICE MOTORCYCLE')).groupBy(df2.CRASH_ID,df2.VEH_BODY_STYL_ID).count().show()

#3
dfnew=df.join(df2,df.CRASH_ID==df2.CRASH_ID).where(df.PRSN_GNDR_ID=='FEMALE').groupBy(df2.VEH_LIC_STATE_ID).count().show()
#4
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, asc,desc
df3=df2.withColumn("TOT_INJRY_CNT",df2["TOT_INJRY_CNT"].cast(IntegerType())).show()

df3.where(col("DEATH_CNT")=="1").groupBy(col("VEH_MAKE_ID")).max("TOT_INJRY_CNT").where(col("max(TOT_INJRY_CNT)")>0).select(col("VEH_MAKE_ID"),col("max(TOT_INJRY_CNT)").alias("MAX_INJRY")).orderBy(col("MAX_INJRY").desc()).show()
from pyspark.sql.types import StructType,IntegerType,StringType,StructField
schema=StructType([StructField('VEH_MAKE_ID',StringType(),True), StructField('MAX_INJRY',IntegerType(),True) ])
dfnew1=spark.createDataFrame(data=df3.collect()[4:14], schema=schema)
