# COMMAND ----------
# MAGIC %md
# MAGIC ## Delta History
# MAGIC Todas las operaciones que hemos realizado sobre el Delta Lake se han registrado en un log que nos permite ver el historico de cambios que se han realizado sobre la tabla.
# MAGIC Además, esto nos permite realizar un rollback a un punto anterior en el tiempo, en caso de que sea necesario
# MAGIC 
# MAGIC

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.circuits

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ingestion_timestamp, processed_timestamp, circuitId, name, location, country FROM silver.circuits

# COMMAND ----------
# MAGIC %md
# MAGIC ## Restaura a un punto anterior en el tiempo.

# COMMAND ----------
# MAGIC %sql
# MAGIC RESTORE silver.circuits TO VERSION AS OF 1

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.circuits

# COMMAND ----------
# MAGIC %sql
# MAGIC RESTORE silver.circuits TO VERSION AS OF 4

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT ingestion_timestamp, processed_timestamp, circuitId, name, location, country FROM silver.circuits


