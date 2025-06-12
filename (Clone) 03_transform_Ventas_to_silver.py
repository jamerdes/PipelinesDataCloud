# Databricks notebook source
# MAGIC %md
# MAGIC ## Reto 3 – Transformación de ventas desde Bronze a Silver
# MAGIC En este notebook se procesa el archivo `sales.csv` desde la capa Bronze,
# MAGIC aplicando limpieza, conversión de tipos, particionamiento por año y mes,
# MAGIC y registro en Unity Catalog.

# COMMAND ----------

#Importaciones park para cast, fechas y manipulación de columnas.
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import IntegerType, DoubleType

#rutas capas Bronze y Silver.
ruta_bronze = "abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/bronze"
ruta_silver = "abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lee el archivo sales.csv desde la capa Bronze del Data Lake.

# COMMAND ----------


# Activa la opción "header" para que Spark reconozca correctamente los nombres de las columnas.
df_sales = spark.read.option("header", True).option("sep", ";").csv(f"{ruta_bronze}/sales.csv")


# Mostra una vista previa con los primeros 5 registros para validar que los datos se han cargado correctamente.

#display(df_sales.limit(5))

df_sales.show(10) 

# COMMAND ----------

# Imprime y muestra el esquema inferido por Spark, lo que nos ayuda a identificar los tipos de datos de cada columna.
df_sales.printSchema()


# COMMAND ----------

# Conteo de registros nulos en las columnas más importantes
df_sales.selectExpr(
    "count(*) as total_registros",
    "sum(case when SalesOrderID is null then 1 else 0 end) as nulos_SalesOrderID",
    "sum(case when OrderQty is null then 1 else 0 end) as nulos_OrderQty",
    "sum(case when LineTotal is null then 1 else 0 end) as nulos_LineTotal",
    "sum(case when SalesOrderDate is null then 1 else 0 end) as nulos_SalesOrderDate"
).show() #.display

# COMMAND ----------

#castea las columnas clave (numéricas y fechas).
# Además eliminamos registros con campos esenciales nulos.

df_sales_clean = (
    df_sales
    .withColumn("SalesOrderID", col("SalesOrderID").cast(IntegerType()))
    .withColumn("ProductID", col("ProductID").cast(IntegerType()))
    .withColumn("OrderQty", col("OrderQty").cast(IntegerType()))
    .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))
    .withColumn("LineTotal", col("LineTotal").cast(DoubleType()))
    .withColumn("SalesOrderDate", to_timestamp("SalesOrderDate"))
    .withColumn("rowguid", col("rowguid").cast("string"))  #si acaso null o mal formatado
)

# COMMAND ----------

# Elimina registros que no tienen clave de pedido ni fecha ni valores monetarios
df_sales_clean = df_sales_clean.na.drop(subset=["SalesOrderID", "SalesOrderDate", "OrderQty", "LineTotal"])


# COMMAND ----------

# Elimina los duplicados basados en la clave primaria, SalesOrderID.
df_sales_clean = df_sales_clean.dropDuplicates(["SalesOrderID"])

# COMMAND ----------

# Genera las columnas year y month a partir de la fecha de la orden.
df_sales_clean = (
    df_sales_clean
    .withColumn("year", year("SalesOrderDate"))
    .withColumn("month", month("SalesOrderDate"))
)

# COMMAND ----------

# Verificación final después de la limpieza
#display(df_sales_clean.limit(5))

df_sales_clean.limit(5).show()

# COMMAND ----------

print("Después de limpieza:", df_sales_clean.count())

# COMMAND ----------


# Imprime el esquema para confirmar los tipos convertidos
df_sales_clean.printSchema()


# COMMAND ----------

# Contamos registros después de limpieza
print("Después de limpieza:", df_sales_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardamos los datos limpios en formato Delta, particionando por año y mes

# COMMAND ----------

df_sales_clean.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .save(f"{ruta_silver}/ventas")

# COMMAND ----------

# Elimina la tabla si ya existe (esto evita errores si estamos rehaciendo el proceso)
spark.sql(f"""
DROP TABLE IF EXISTS adbmde01jamerdes_1132885985894215.silver.ventas
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registramos la tabla Delta en el esquema silver del Unity Catalog

# COMMAND ----------

# Registra la tabla de ventas en el catálogo Unity
spark.sql(f"""
CREATE TABLE adbmde01jamerdes_1132885985894215.silver.ventas
USING DELTA
LOCATION '{ruta_silver}/ventas'
""")

# COMMAND ----------

# Visualizamos registros desde la tabla registrada
#spark.sql("SELECT * FROM silver.ventas").display()

spark.sql("SELECT * FROM silver.ventas").show()
