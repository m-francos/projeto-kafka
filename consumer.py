from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType, TimestampType
from py4j.protocol import Py4JError

spark = SparkSession.builder \
    .appName("Consumidor_Kafka_Mercado") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_servers = "localhost:9092"
topic_vendas = "vendas_ecommerce"

esquema = StructType([
    StructField("id_ordem", StringType()),
    StructField("documento_cliente", StringType()),
    StructField("produtos_comprados", ArrayType(StructType([
        StructField("nome_produto", StringType()),
        StructField("quantidade", IntegerType()),
        StructField("preco_unitario", FloatType())
    ]))),
    StructField("data_hora_venda", TimestampType())
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", topic_vendas) \
    .option("startingOffsets", "earliest") \
    .load()

df_stream = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema).alias("data")) \
    .select("data.*")

df_expandido = df_stream.withColumn("produto", col("produtos_comprados").getItem(0).getField("nome_produto")) \
    .withColumn("quantidade", col("produtos_comprados").getItem(0).getField("quantidade")) \
    .withColumn("preco_unitario", col("produtos_comprados").getItem(0).getField("preco_unitario"))

df_expandido = df_expandido.withColumn("valor_venda", col("quantidade") * col("preco_unitario"))

df_agregado = df_expandido.groupBy("produto") \
    .agg(
        spark_sum("quantidade").alias("quantidade_total"),
        spark_round(spark_sum("valor_venda"), 2).alias("valor_total_vendas")
    )

try:
    print("\033[92m" + "\nIniciando o consumo de dados de vendas...\n" + "\033[0m")
    
    consulta = df_agregado.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    consulta.awaitTermination()

except KeyboardInterrupt:
    print("\033[93m" + "\nConsumo interrompido pelo usuário.\n" + "\033[0m")

except Py4JError as erro:
    if "awaitTermination" in str(erro):
        print("\033[93m" + "\nConsumo interrompido.\n" + "\033[0m")
    else:
        print("\033[91m" + f"\nErro ao consumir os dados: {erro}\n" + "\033[0m")

except Exception as erro:
    print("\033[91m" + f"\nErro inesperado: {erro}\n" + "\033[0m")

finally:
    print("\033[92m" + "Processo finalizado.\n" + "\033[0m")
    spark.stop()

