def transformar_silver(caminho_local):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import arrays_zip, explode, concat, lit

    spark = SparkSession.builder.appName("ClimaLake").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.option("multiLine",True).json(caminho_local)

    df_horarios = df.withColumn(
        "Horarios",
        explode(arrays_zip(
            df["hourly"]["time"],
            df["hourly"]["temperature_2m"],
            df["hourly"]["precipitation"],
            df["hourly"]["relative_humidity_2m"]
        ))
    )

    df_horarios = df_horarios.select(
        df_horarios["Horarios"]["time"].alias("Horário"),
        df_horarios["Horarios"]["temperature_2m"].alias("Temperatura °C"),
        df_horarios["Horarios"]["precipitation"].alias("Precipitação (mm)"),
        concat(
            df_horarios["Horarios"]["relative_humidity_2m"].cast("string"),
            lit("%")
        ).alias("Humidade Relativa do Ar")
    )

    return df_horarios

def transformar_gold(caminho_local):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, min, max, sum, regexp_replace, round, min_by, max_by, date_format, concat, lit
    from pyspark.sql.types import DoubleType


    spark = SparkSession.builder.appName("App").getOrCreate()

    df_silver = spark.read.option("multiline",True).json(caminho_local)

    df = df_silver.groupBy().agg(
        round(min("Temperatura °C").cast(DoubleType()),1).alias("Temperatura Mínima °C"),
        date_format(min_by(df_silver["Horário"],df_silver["Temperatura °C"]),"yyyy-MM-dd HH:mm").alias("Horário Temperatura Mínima °C"),

        round(max("Temperatura °C").cast(DoubleType()),1).alias("Temperatura Máxima °C"),
        date_format(max_by(df_silver["Horário"],df_silver["Temperatura °C"]),"yyyy-MM-dd HH:mm").alias("Horário Temperatura Máxima °C"),

        round(avg("Temperatura °C").cast(DoubleType()),1).alias("Temperatura Média °C"),

        round(sum("Precipitação (mm)").cast(DoubleType()),1).alias("Precipitação Total (mm)"),

        concat(round(min(regexp_replace("Humidade Relativa do Ar","%","").cast(DoubleType())),0).cast("int").cast("string"),lit("%")).alias("Humidade Relativa do Ar Mínima"),
        date_format(min_by(df_silver["Horário"],regexp_replace("Humidade Relativa do Ar","%","").cast(DoubleType())),"yyyy-MM-dd HH:mm").alias("Horário Humidade Relativa do Ar Mínima °C"),

        concat(round(max(regexp_replace("Humidade Relativa do Ar","%","").cast(DoubleType())),0).cast("int").cast("string"),lit("%")).alias("Humidade Relativa do Ar Máxima"),
        date_format(max_by(df_silver["Horário"],regexp_replace("Humidade Relativa do Ar","%","").cast(DoubleType())),"yyyy-MM-dd HH:mm").alias("Horário Humidade Relativa do Ar Máxima °C"),

        concat(round(avg(regexp_replace("Humidade Relativa do Ar","%","").cast(DoubleType())),0).cast("int").cast("string"),lit("%")).alias("Humidade Relativa do Ar Média")
    )

    # Amplitude (Temperatura Máxima - Temperatura Mínima)
    df = df.withColumn(
        "Amplitude Térmica", 
        round(df["Temperatura Máxima °C"] - df["Temperatura Mínima °C"],1)
    )

    return df