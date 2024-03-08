# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingesta de datos desde la capa Bronze a la capa Silver
# MAGIC
# MAGIC Ingestaremos los datos de la capa Bronze a la capa Silver.
# MAGIC Seguiremos usando el fichero de metadatos que utilizamos en el ejercicio anterior
# MAGIC

# COMMAND ----------
# MAGIC %md
# MAGIC ### Definimos a continuacion una serie de funciones que nos permitiran leer el fichero de metadatos y leer y escribir los datos en el formato deseado.
# MAGIC La logica al leer ficheros y al escribir es distinta, dado que en Silver queremos mantener la version mas actualizada de los datos, 
# MAGIC por lo que usaremos la operacion `merge` de Delta Lake para actualizar los datos existentes y añadir los nuevos (**UPSERT**).
# MAGIC

# COMMAND ----------
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import List

spark_session = SparkSession.builder.appName("silver_pipeline").getOrCreate()

SOURCE_SCHEMA="bronze"
SCHEMA="silver"
SOURCE_PATH = "dbfs:/FileStore/tables/datasets"
SILVER_PATH = f"dbfs:/FileStore/tables/f1/{SCHEMA}"

METADATA_PATH = f"{SOURCE_PATH}/table_metadata.yaml"

def read_yml_file(file_path:str):
    file_content=dbutils.fs.head(file_path)
    return yaml.safe_load(file_content)
    
def read_table(spark_session:SparkSession, table:str, file_path:str):
    # cargamos los datos desde el esquema de bronze, seleccionando la fecha de ingesta mas reciente
    source_df=spark_session.read.format("delta").table(f"{SOURCE_SCHEMA}.{table}")
    date_column_name="ingestion_timestamp"
    max_date_df = source_df.agg(F.max(F.col(date_column_name)).alias(date_column_name))
    joined_df=source_df.join(max_date_df, on=date_column_name, how="inner")
    print(f"Maximum bronze ingestion date found is: {max_date_df.collect()[0][0].strftime('%Y-%m-%d %H:%m:%s')}")
    print(f"{source_df.count()} records found in {SOURCE_SCHEMA}.{table}. Filtering by max date will take {joined_df.count()} rows...")
    return joined_df
    
    
def write_table(spark_session:SparkSession, df:DataFrame, table:str, table_path:str, schema_name:str, pk_columns:List[str], mode:str="append"):
    # write a dataframe to a delta table
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    
    deltaTable = (
            DeltaTable.createIfNotExists(spark_session)
            .location(table_path)
            .tableName(f"{schema_name}.{table}")
            .addColumns(df.schema)
            .execute()
    )
    merge_string = ' AND '.join([f"existing.{col}=incoming.{col}" for col in pk_columns])
    deltaTable.alias("existing").merge(df.alias("incoming"), merge_string) \
                                .whenMatchedUpdateAll() \
                                .whenNotMatchedInsertAll() \
                                .execute()
                                
def transform_data(df:DataFrame):
    return df.withColumn("processed_timestamp", F.current_timestamp())

# COMMAND ----------
# MAGIC %md
# MAGIC ### Leemos el fichero de metadatos
# MAGIC

# COMMAND ----------
metadata = read_yml_file(METADATA_PATH)
print("Metadata file loaded!")
print(metadata)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Iteramos sobre cada tabla y aplicamos el siguiente pipeline:
# MAGIC - 1. Leer el archivo de origen
# MAGIC - 2. Aplicamos la logica de transformacion sobre los datos que se han leido desde la capa Bronze
# MAGIC - 3. Guardamos el archivo transformado en formato Delta
# MAGIC

# COMMAND ----------
for table in metadata["tables"]:
    table_name = table["table_name"]
    file_name = table["file_name"]
    primary_key = table["primary_key"]
    
    print(f"Reading data from {SOURCE_SCHEMA}.{table_name}...")
    df = read_table(spark_session, table_name, f"{SOURCE_PATH}/{file_name}")
    
    print(f"Applying transformation logic to {table_name}...")
    transformed_df = transform_data(df)
    
    print(f"Writing data to Delta Table {SCHEMA}.{table_name}...")
    write_table(spark_session, transformed_df, table_name, f"{SILVER_PATH}/{table_name}", SCHEMA, primary_key)
    
# COMMAND ----------
# MAGIC %md
# MAGIC ### Ahora que hemos cargado los datos en formato Delta, podemos realizar consultas y analisis sobre ellos.
# MAGIC
# MAGIC Hemos creado una base de datos (schema), **silver**, en la que se almacenan los datos procedentes de la capa bronze filtrados por la ultima ingesta.
# MAGIC La logica de la transformacion es simple para este ejemplo, y consiste en añadir una columna con la fecha de procesamiento de los datos.
# MAGIC
# MAGIC Podemos usar consultas sobre ellos para realizar analisis exploratorio, limpieza de datos, etc.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ingestion_timestamp, processed_timestamp, circuitId, name, location, country FROM silver.circuits

# COMMAND ----------
drivers = spark_session.read.format("delta").table("silver.drivers").select("driverId", "driverRef", "number","code","forename","processed_timestamp", "ingestion_timestamp")
display(drivers)