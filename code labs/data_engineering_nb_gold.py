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

spark_session = SparkSession.builder.appName("bronze_pipeline").getOrCreate()
SOURCE_PATH = "dbfs:/FileStore/tables/datasets"
SILVER_PATH = f"{SOURCE_PATH}/f1/silver"

METADATA_PATH = f"{SOURCE_PATH}/metadata.yml"

SOURCE_SCHEMA="bronze"
SCHEMA="silver"

def read_yml_file(file_path:str):
    file_content=dbutils.fs.head(file_path)
    return yaml.safe_load(file_content)
    
def read_table(spark_session:SparkSession, table:str, file_path:str):
    # cargamos los datos desde el esquema de bronze, seleccionando la fecha de ingesta mas reciente
    return (spark_session.read.format("delta").table(f"{SOURCE_SCHEMA}.{table}"))
    
    
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
# MAGIC ### Leemos los datos en CSV desde la ruta de origen usando el fichero de metadatos
# MAGIC

# COMMAND ----------
metadata = read_yml_file(METADATA_PATH)
print("Metadata file loaded!")
print(metadata)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Iteramos sobre cada tabla y aplicamos el siguiente pipeline:
# MAGIC - 1. Leer el archivo de origen
# MAGIC - 2. Aplicamos la logica de transformacion sobre el archivo de origen
# MAGIC - 3. Guardamos el archivo transformado en formato Delta
# MAGIC

# COMMAND ----------
for table in metadata["tables"]:
    table_name = table["table_name"]
    file_name = table["file_name"]
    primary_key = table["primary_key"]
    
    print(f"Reading {file_name} into {table_name}...")
    df = read_table(spark_session, table_name, f"{SOURCE_PATH}/{file_name}")
    
    print(f"Applying transformation logic to {table_name}...")
    transformed_df = transform_data(df)
    
    row_count = transformed_df.count()
    
    print(f"Writing {row_count} rows to Delta Table {SCHEMA}.{table_name}...")
    write_table(spark_session, transformed_df, table_name, f"{SILVER_PATH}/{table_name}", SCHEMA)
