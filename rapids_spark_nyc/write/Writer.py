df.write.format("delta").mode("append").saveAsTable("default.people10m")

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("default.people10m")