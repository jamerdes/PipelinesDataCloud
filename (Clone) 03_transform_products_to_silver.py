# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Reto 3 – Transformación de productos desde Bronze a Silver
# MAGIC
# MAGIC Este notebook se procesa el archivo `products.csv` desde la capa Bronze,
# MAGIC aplicando limpieza, conversión de tipos y registro en Unity Catalog.

# COMMAND ----------

#Importaciones
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp


#Rutas
ruta_bronze = "abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/bronze"
ruta_silver = "abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargué los datos desde la capa Bronze. Este archivo contiene la información de productos que será limpiada y estandarizada.
# MAGIC

# COMMAND ----------

# lee el archivo products.csv desde la carpeta Bronze del Data Lake. además se activa la opción "header" para que Spark interprete correctamente los nombres de las columnas.
df_products = spark.read.option("header", True).csv(f"{ruta_bronze}/products.csv")

# Se muestran las primeras 5 filas del DataFrame para verificar que los datos fueron cargados correctamente.

#display(df_products.limit(5))
df_products.show(15)

# COMMAND ----------

# se imprime el esquema inferido por Spark para identificar los tipos de datos.
df_products.printSchema()

# COMMAND ----------

print("Antes de limpieza:", df_products.count())

# COMMAND ----------

#Nombre Columnas
print(df_products.columns)


# COMMAND ----------

# Estadística descriptiva del DataFrame antes de la limpieza

#df_products.describe(["ProductID", "ProductCategoryID", "Weight", "SellStartDate"]).display()
df_products.describe(["ProductID", "ProductCategoryID", "Weight", "SellStartDate"]).show()

# COMMAND ----------

# %md
# Validación de valores nulos en columnas clave

df_products.selectExpr(
    "count(*) as total_registros",
    "sum(case when ProductID is null then 1 else 0 end) as nulos_ProductID",
    "sum(case when ProductCategoryID is null then 1 else 0 end) as nulos_ProductCategoryID",
    "sum(case when Weight is null then 1 else 0 end) as nulos_Weight",
    "sum(case when SellStartDate is null then 1 else 0 end) as nulos_SellStartDate"
).show()#.display


# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza

# COMMAND ----------

# Realiza el cast de la columna ProductID al tipo integerType para asegurar consistencia en futuras operaciones analíticas.
df_products_casted = df_products.withColumn("ProductID", col("ProductID").cast(IntegerType()))

# Elimina únicamente las filas donde ProductID está nulo, ya que representan registros incompletos e inválidos para el análisis.
df_products_clean = df_products_casted.na.drop(subset=["ProductID"])

# COMMAND ----------

## Conversiones de tipo a las columnas numéricas y de fecha. 
# Esto es esencial para asegurar coherencia en las siguientes capas analíticas...
df_products_clean = (
    df_products
    .withColumn("ProductID", col("ProductID").cast("int"))
    .withColumn("ProductCategoryID", col("ProductCategoryID").cast("int"))
    .withColumn("ProductModelID", col("ProductModelID").cast("int"))
    .withColumn("Weight", col("Weight").cast("double"))
    .withColumn("SellStartDate", to_timestamp("SellStartDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
)


# COMMAND ----------

## Eliminar los registros con productID nulo, ya que no pueden ser usados para análisis ni cálculos posteriores.

df_products_clean = df_products_clean.filter(col("ProductID").isNotNull())

# COMMAND ----------

# Eliminamos posibles duplicados basados en la clave primaria ProductID.
df_products_clean = df_products_clean.dropDuplicates(["ProductID"])


# COMMAND ----------

#Muestra los datos limpios
# display(df_products_clean.limit(5))

df_products_clean.show(15)

# COMMAND ----------

# Mostra el conteo de registros antes y después de la limpieza.

print("Después de limpieza:", df_products_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genera columnas auxiliares 'year' y 'month' a partir de la fecha de inicio de venta. Esto permitirá una partición eficiente en consultas en silver o gold.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import year, month

df_products_clean = (
    df_products_clean
    .withColumn("year", year("SellStartDate"))
    .withColumn("month", month("SellStartDate"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ahora guardamos los datos limpios y transformados en formato Delta en la capa Silver, particionando por año y mes.

# COMMAND ----------

# Guardalos datos transformados en formato Delta en la capa Silver.
# Particionados por año y mes, que ya fueron definidos en el DataFrame anteriormente.
# Usamos la ruta dinámica definida como variable ruta_silver
df_products_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{ruta_silver}/productos")

# COMMAND ----------

# Alternativamente, escribimos directamente en el path montado (/mnt) si estuviera configurado.
# Esto no es necesario si ya usamos abfss:// directamente, pero puede ser útil para pruebas locales.
df_products_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
  .save("/mnt/datalake/silver/productos")


#la tabla Delta en el Unity Catalog si no existe aún.
# Esto permite consultar los datos desde SQL o Python como tabla externa.
spark.sql("""
  CREATE TABLE IF NOT EXISTS silver.productos
  USING DELTA
  LOCATION 'abfss://datalake@dlsmde01jamerdess.dfs.core.windows.net/silver/productos'
""")


# COMMAND ----------

# mprime el esquema dataframe limpios..
df_products_clean.printSchema()

# COMMAND ----------

# Elimina la tabla si ya existe
spark.sql(f"DROP TABLE IF EXISTS adbmde01jamerdes_1132885985894215.silver.productos")

# Criar a tabela Delta no catálogo Unity
spark.sql(f"""
CREATE TABLE adbmde01jamerdes_1132885985894215.silver.productos
USING DELTA
LOCATION '{ruta_silver}/productos'
""")


# COMMAND ----------

# Usamos este comando para listar todos los catálogos disponibles en el entorno Databricks.
# Esto permite verificar que el Unity Catalog está activo y cuál es el nombre real del catálogo del alumno.
spark.sql('SHOW CATALOGS')


# COMMAND ----------

# MAGIC %md
# MAGIC **Consulta SQL directamente sobre la tabla registrada en el esquema silver.
# MAGIC Esto nos permite comprobar que la tabla Delta fue registrada correctamente en el Unity Catalog
# MAGIC y que los datos están disponibles para análisis desde cualquier notebook.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.productos

# COMMAND ----------

#muestra la tabla registrada en unity catalog en delta
spark.sql("SELECT * FROM silver.productos").show()
