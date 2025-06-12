# Databricks notebook source
# MAGIC %md
# MAGIC ##Reto 5 – Cálculos en la capa Gold
# MAGIC
# MAGIC En este notebook realizamos un procesamiento sobre los datos de la capa **Silver** para generar una tabla consolidada de ventas e ingresos por producto.
# MAGIC
# MAGIC Se calculan las métricas:
# MAGIC - Total_sold: cantidad total vendida por producto
# MAGIC - Total_revenue: ingresos totales generados por producto
# MAGIC
# MAGIC El resultado se guarda como una tabla Delta en la **Capa Gold** con el nombre Informeventas, registrada en el Unity Catalog.

# COMMAND ----------

#Importaciones
from pyspark.sql.functions import sum as _sum

# COMMAND ----------

# Lee las tablas desde el Unity Catalog
df_ventas = spark.table("silver.ventas")
df_productos = spark.table("silver.productos")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Carga de datos desde la capa Silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **En este paso, cargamos las dos tablas disponibles en la capa Silver**:
# MAGIC
# MAGIC - silver.ventas: contiene los registros de ventas  
# MAGIC - silver.productos: contiene los detalles de los productos

# COMMAND ----------

#Muestra la tabla ventas
df_ventas.display(5)



# COMMAND ----------

#Muestra el esquema ventas
df_ventas.printSchema()

# COMMAND ----------

#Muestra la tabla productos
df_productos.display(5)


# COMMAND ----------

#Muestra el esquema productos
df_productos.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ahora empezamos los análises de datos
# MAGIC
# MAGIC Validamos la estructura y el contenido de ambas tablas antes de procesar.
# MAGIC

# COMMAND ----------

# Lee las tablas de la capa Silver previamente registradas en el Unity Catalog
df_ventas = spark.table("silver.ventas")
df_productos = spark.table("silver.productos")

# COMMAND ----------

# Calculamos las métricas de ventas agrupadas por ProductID
df_metrics = (
    df_ventas.groupBy("ProductID")
    .agg(
    _sum("OrderQty").alias("total_sold"),
    _sum("LineTotal").alias("total_revenue")
    )
)

# COMMAND ----------

df_metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de métricas sobre las ventas
# MAGIC
# MAGIC Calculamos dos métricas clave por producto:
# MAGIC
# MAGIC - Total_sold: total de unidades vendidas  
# MAGIC - Total_revenue: ingresos totales
# MAGIC

# COMMAND ----------

# Aplicacion join entre tabla de productos para obtener el nombre de cada producto
df_final = (
    df_metrics.join(df_productos, df_metrics.ProductID == df_productos.ProductID, "inner")
    .select(
        df_metrics.ProductID.alias("product_id"),
        df_productos.ProductName.alias("product_name"),
        "total_sold",
        "total_revenue"
    )
)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unión con la tabla de productos
# MAGIC
# MAGIC
# MAGIC Unimos las métricas con el nombre del producto para tener todos los informes en una sola tabla como resultado final.
# MAGIC

# COMMAND ----------

# Define la ruta en el Data Lake para la capa Gold
ruta_gold = "abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/gold"

# Guarda el DataFrame resultante en formato Delta, sobrescribiendo si ya existe
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{ruta_gold}/informeventas")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Escritura del resultado y Registro de la tabla en el Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC Guardamos el DataFrame resultante como tabla Delta en la ruta **/gold/informeventas**.

# COMMAND ----------

# Creacion del esquema 'gold' en el catálogo si aún no existe
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# Registra la tabla como externa en el Unity Catalog, apuntando a la ubicación en la capa Gold
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.informeventas
USING DELTA
LOCATION 'abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/gold/informeventas'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Esta tabla permite análisis agregados de ventas por producto sin necesidad de realizar joins adicionales, facilitando dashboards y reportes.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.informeventas

# COMMAND ----------


