# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingesta de datos desde la capa Silver a la capa Gold
# MAGIC
# MAGIC Vamos a generar ahora 2 tablas en la capa Gold, generando una serie de agregados y calculos sobre los datos de la capa Silver.
# MAGIC Los guardaremos para posteriomente consultarlos con SQL y generar visualizaciones. Las agregaciones seran las siguientes:
# MAGIC - **most_awarded_drivers**: Tabla con los pilotos que han ganado mas carreras
# MAGIC - **most_pole_position_drivers**: Tabla con los pilotos que han conseguido mas pole positions
# MAGIC

# COMMAND ----------
# MAGIC %md
# MAGIC ### Definimos a continuacion una serie de funciones que nos permitiran leer y escribir los datos en el formato deseado.
# MAGIC En este caso sobreescribiremos las tablas en caso de que existan, ya que siempre vamos a regenerar los datos.


# COMMAND ----------
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import List

spark_session = SparkSession.builder.appName("gold_pipeline").getOrCreate()
SOURCE_PATH = "dbfs:/FileStore/tables/datasets"
GOLD_PATH = f"{SOURCE_PATH}/f1/gold"

METADATA_PATH = f"{SOURCE_PATH}/metadata.yml"

SOURCE_SCHEMA="silver"
SCHEMA="gold"

def read_table(spark_session:SparkSession, table:str):
    # cargamos los datos desde el esquema de bronze, seleccionando la fecha de ingesta mas reciente
    return (spark_session.read.format("delta").table(f"{SOURCE_SCHEMA}.{table}"))
    
    
def write_table(spark_session:SparkSession, df:DataFrame, table:str, table_path:str, schema_name:str):
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    
    # sobreescribimos la tabla en caso de que exista, siempre vamos a regenerar los datos
    (df.write.format("delta")
     .option("path", table_path)
     .mode("overwrite")
     .saveAsTable(f"{schema_name}.{table}"))
                                
# COMMAND ----------
# MAGIC %md
# MAGIC ### Definimos la tabla **most_awarded_drivers** que contendra los pilotos que han ganado mas carreras
# MAGIC

# COMMAND ----------
def generate_most_awarded_drivers():

    drivers_df = read_table(spark_session,"drivers")
    results_df = read_table(spark_session,"results")
    filtered_results = results_df.filter(F.col("position") == 1)    
    joined_df = drivers_df.join(filtered_results, drivers_df.driverId == results_df.driverId)

    
    grouped_df = joined_df.groupBy(drivers_df.driverId, drivers_df.driverRef, drivers_df.code, drivers_df.forename, drivers_df.surname) \
                        .agg(F.count(drivers_df.driverId).alias("total_wins"))

    final_df = grouped_df.withColumn("driver_name", F.concat(F.col("forename"), F.lit(" "), F.col("surname")))

    return final_df.select("driverId", "driverRef", "code", "driver_name", "total_wins")

# COMMAND ----------
def generate_most_pole_position_drivers():
    drivers_df = read_table(spark_session,"drivers")
    qualifying_df = read_table(spark_session,"qualifying")

    # Filter for position = 1 in qualifying results
    filtered_qualifying = qualifying_df.filter(F.col("position") == 1)

    # Join the filtered qualifying DataFrame with the drivers DataFrame
    joined_df = drivers_df.join(filtered_qualifying, "driverId")

    # Group by the necessary driver fields
    grouped_df = joined_df.groupBy(drivers_df.driverId, drivers_df.forename, drivers_df.surname) \
                        .agg(F.count("*").alias("total_pole_positions"))

    # Create the driver_name column
    final_df = grouped_df.withColumn("driver_name", F.concat(F.col("forename"), F.lit(" "), F.col("surname")))

    # Select the desired columns
    return final_df.select("driver_name", "total_pole_positions")


# COMMAND ----------
# MAGIC %md
# MAGIC ### Iteramos sobre cada tabla y aplicamos el siguiente pipeline:
# MAGIC - 1. Leer el archivo de origen
# MAGIC - 2. Aplicamos la logica de transformacion sobre el archivo de origen
# MAGIC - 3. Guardamos el archivo transformado en formato Delta
# MAGIC

# COMMAND ----------
most_awarded_drivers_df = generate_most_awarded_drivers()
write_table(spark_session, most_awarded_drivers_df, "most_awarded_drivers", f"{GOLD_PATH}/most_awarded_drivers", SCHEMA)

most_pole_position_drivers_df = generate_most_pole_position_drivers()
write_table(spark_session, most_pole_position_drivers_df, "most_pole_position_drivers", f"{GOLD_PATH}/most_pole_position_drivers", SCHEMA)