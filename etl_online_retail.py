"""
Taller 1 - ETL con PySpark (Online Retail Dataset)
====================================================

Este script aplica un proceso ETL completo sobre el dataset "Online Retail"
(UCI Machine Learning Repository) usando PySpark, y responde las 10 preguntas
del taller.

Cómo ejecutarlo (ver README.md para más detalle):
    python etl_online_retail.py

Requisitos:
    - Java 8/11/17 instalado
    - pip install pyspark
    - El archivo "Online Retail.csv" en la carpeta ./data
"""

import os
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------------------------------------------------
# 0. CONFIGURACIÓN GENERAL
# -----------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "Online Retail.csv")
EXCEL_PATH = os.path.join(BASE_DIR, "data", "Online Retail.xlsx")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
# Si OneDrive o una política de Windows bloquea temporales en el proyecto,
# puede indicarse otra ruta con SPARK_LOCAL_DIR.
TEMP_DIR = os.environ.get("SPARK_LOCAL_DIR", os.path.join(BASE_DIR, ".spark_tmp"))
HADOOP_HOME = os.path.join(BASE_DIR, ".hadoop")
WINUTILS_PATH = os.path.join(HADOOP_HOME, "bin", "winutils.exe")
HADOOP_DLL_PATH = os.path.join(HADOOP_HOME, "bin", "hadoop.dll")
# UTF-8 es el formato recomendado al convertir el Excel original. Si se usa un
# CSV Latin-1, ejecutar con CSV_ENCODING=ISO-8859-1.
CSV_ENCODING = os.environ.get("CSV_ENCODING", "UTF-8")
REQUIRED_COLUMNS = {
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
}

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
# PySpark crea un archivo temporal para iniciar la pasarela Java. Usar una
# carpeta del proyecto evita fallos de permisos en el TEMP global de Windows.
os.environ["TEMP"] = TEMP_DIR
os.environ["TMP"] = TEMP_DIR

# Hadoop necesita winutils.exe para crear carpetas de salida en Windows.
# Se mantiene como dependencia local, sin modificar variables globales.
if os.name == "nt":
    if not os.path.isfile(WINUTILS_PATH) or not os.path.isfile(HADOOP_DLL_PATH):
        raise FileNotFoundError(
            "Faltan .hadoop/bin/winutils.exe o .hadoop/bin/hadoop.dll. "
            "Consulta el README para descargarlos."
        )
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + os.pathsep + os.environ["PATH"]


def preparar_dataset():
    """Convierte automáticamente el Excel oficial a CSV cuando está disponible."""
    if os.path.exists(EXCEL_PATH):
        try:
            from openpyxl import load_workbook
        except ImportError as exc:
            raise RuntimeError(
                "Se encontró Online Retail.xlsx. Instala openpyxl con: "
                "pip install openpyxl"
            ) from exc

        print("Convirtiendo data/Online Retail.xlsx a CSV UTF-8...")
        workbook = load_workbook(EXCEL_PATH, read_only=True, data_only=True)
        worksheet = workbook.active
        with open(DATA_PATH, "w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            for row in worksheet.iter_rows(values_only=True):
                writer.writerow(row)
        workbook.close()
    elif not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            "No se encontró data/Online Retail.csv ni data/Online Retail.xlsx. "
            "Descarga el dataset oficial de UCI y colócalo dentro de data/."
        )


preparar_dataset()


def guardar_csv(df, nombre):
    """
    Guarda un DataFrame de Spark como CSV dentro de output/<nombre>.
    Spark escribe una CARPETA con archivos part-*.csv adentro (comportamiento normal
    de Spark en modo distribuido), no un único archivo .csv.
    """
    ruta = os.path.join(OUTPUT_DIR, nombre)
    (
        df.coalesce(1)                     # 1 solo archivo de salida (dataset pequeño)
          .write.mode("overwrite")
          .option("header", "true")
          .csv(ruta)
    )
    print(f"Guardado -> {ruta}/")


def mostrar_titulo(texto):
    print("\n" + "=" * 80)
    print(texto)
    print("=" * 80)


# -----------------------------------------------------------------------
# 1. INICIALIZAR SPARK
# -----------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Taller ETL - Online Retail")
    .master("local[*]")
    .config("spark.driver.extraJavaOptions", f"-Djava.library.path={os.path.join(HADOOP_HOME, 'bin')}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")  # reduce ruido en consola


# -----------------------------------------------------------------------
# 2. LECTURA DE DATOS  (spark.read.format("csv"))
# -----------------------------------------------------------------------

mostrar_titulo("1. LECTURA DE DATOS")

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("encoding", CSV_ENCODING)
    .load(DATA_PATH)
)

missing_columns = REQUIRED_COLUMNS.difference(df_raw.columns)
if missing_columns:
    raise ValueError(
        "El CSV no tiene las columnas requeridas: "
        + ", ".join(sorted(missing_columns))
    )

print("Esquema original:")
df_raw.printSchema()
print(f"Filas leídas: {df_raw.count()}")


# -----------------------------------------------------------------------
# 3. LIMPIEZA Y TRANSFORMACIÓN (withColumn, select, filter)
# -----------------------------------------------------------------------

mostrar_titulo("2. LIMPIEZA Y TRANSFORMACIÓN")

df = (
    df_raw
    .select(
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    )
    # Convertir tipos por si inferSchema no detectó bien algo
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    # El Excel oficial usa M/d/yyyy H:mm; se admite además el formato ISO
    # frecuente al exportar CSV desde otras herramientas.
    .withColumn(
        "InvoiceDate",
        F.coalesce(
            F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"),
            F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"),
        ),
    )
    # Columna derivada: monto de la línea = Quantity * UnitPrice
    .withColumn("Amount", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    # Columna derivada: mes de la venta (para la pregunta 9)
    .withColumn("YearMonth", F.date_format("InvoiceDate", "yyyy-MM"))
    # Columna derivada: marca si la factura es una devolución (Quantity negativo)
    .withColumn("EsDevolucion", F.when(F.col("Quantity") < 0, True).otherwise(False))
)

# Quitamos filas sin descripción o sin precio válido (registros corruptos típicos de este dataset)
df = df.filter(F.col("UnitPrice").isNotNull())

if df.limit(1).count() == 0:
    raise ValueError("No quedaron filas válidas después de la limpieza.")

df.cache()
print("Muestra de datos limpios:")
df.show(5, truncate=False)


# =========================================================================
# PREGUNTAS DEL TALLER
# =========================================================================

resultados_texto = []  # aquí acumulamos las respuestas para el documento de conclusiones


# -----------------------------------------------------------------------
# Pregunta 1: Número total de facturas
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 1: Número total de facturas")

total_facturas = df.select("InvoiceNo").distinct().count()
print(f"Total de facturas: {total_facturas}")
resultados_texto.append(f"1. Número total de facturas: {total_facturas}")

pd_p1 = spark.createDataFrame([(total_facturas,)], ["total_facturas"])
guardar_csv(pd_p1, "01_total_facturas")


# -----------------------------------------------------------------------
# Pregunta 2: Número de clientes únicos
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 2: Número de clientes únicos")

total_clientes = df.filter(F.col("CustomerID").isNotNull()).select("CustomerID").distinct().count()
print(f"Total de clientes únicos: {total_clientes}")
resultados_texto.append(f"2. Número de clientes únicos: {total_clientes}")

pd_p2 = spark.createDataFrame([(total_clientes,)], ["clientes_unicos"])
guardar_csv(pd_p2, "02_clientes_unicos")


# -----------------------------------------------------------------------
# Pregunta 3: Ingreso total (Quantity * UnitPrice)
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 3: Ingreso total")

ingreso_total = df.filter(F.col("Quantity") > 0).agg(F.round(F.sum("Amount"), 2).alias("ingreso_total")).collect()[0][0]
print(f"Ingreso total (solo ventas, sin devoluciones): {ingreso_total}")
resultados_texto.append(f"3. Ingreso total (ventas positivas): {ingreso_total}")

pd_p3 = spark.createDataFrame([(ingreso_total,)], ["ingreso_total"])
guardar_csv(pd_p3, "03_ingreso_total")


# -----------------------------------------------------------------------
# Pregunta 4: Producto más vendido en cantidad (groupBy + agg + orderBy)
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 4: Producto más vendido en cantidad")

producto_top = (
    df.filter(F.col("Quantity") > 0)
    .groupBy("StockCode", "Description")
    .agg(F.sum("Quantity").alias("cantidad_total"))
    .orderBy(F.desc("cantidad_total"))
)
producto_top.show(5, truncate=False)
resultados_texto.append(
    f"4. Producto más vendido: {producto_top.first()['Description']} "
    f"({producto_top.first()['cantidad_total']} unidades)"
)
guardar_csv(producto_top, "04_producto_mas_vendido")


# -----------------------------------------------------------------------
# Pregunta 5: Cliente con mayor volumen de compra en dinero
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 5: Cliente con mayor volumen de compra")

cliente_top = (
    df.filter((F.col("Quantity") > 0) & (F.col("CustomerID").isNotNull()))
    .groupBy("CustomerID")
    .agg(F.round(F.sum("Amount"), 2).alias("monto_total"))
    .orderBy(F.desc("monto_total"))
)
cliente_top.show(5, truncate=False)
resultados_texto.append(
    f"5. Cliente con mayor compra: CustomerID {cliente_top.first()['CustomerID']} "
    f"(${cliente_top.first()['monto_total']})"
)
guardar_csv(cliente_top, "05_cliente_mayor_compra")


# -----------------------------------------------------------------------
# Pregunta 6: Top 5 países que más compran fuera de Reino Unido
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 6: Top 5 países (fuera de UK)")

paises_top = (
    df.filter((F.col("Quantity") > 0) & (F.col("Country") != "United Kingdom"))
    .groupBy("Country")
    .agg(F.round(F.sum("Amount"), 2).alias("monto_total"))
    .orderBy(F.desc("monto_total"))
    .limit(5)
)
paises_top.show(truncate=False)
resultados_texto.append("6. Top 5 países fuera de UK: ver output/06_top_paises")
guardar_csv(paises_top, "06_top_paises")


# -----------------------------------------------------------------------
# Pregunta 7: Ticket promedio por factura
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 7: Ticket promedio por factura")

ticket_por_factura = (
    df.filter(F.col("Quantity") > 0)
    .groupBy("InvoiceNo")
    .agg(F.round(F.sum("Amount"), 2).alias("total_factura"))
)
ticket_promedio = ticket_por_factura.agg(F.round(F.avg("total_factura"), 2).alias("ticket_promedio")).collect()[0][0]
print(f"Ticket promedio por factura: {ticket_promedio}")
resultados_texto.append(f"7. Ticket promedio por factura: {ticket_promedio}")

pd_p7 = spark.createDataFrame([(ticket_promedio,)], ["ticket_promedio"])
guardar_csv(pd_p7, "07_ticket_promedio")


# -----------------------------------------------------------------------
# Pregunta 8: Mínimo, máximo y promedio de productos (líneas) por factura
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 8: Min/Max/Promedio de productos por factura")

productos_por_factura = (
    df.groupBy("InvoiceNo")
    .agg(F.count("StockCode").alias("num_productos"))
)
stats_p8 = productos_por_factura.agg(
    F.min("num_productos").alias("minimo"),
    F.max("num_productos").alias("maximo"),
    F.round(F.avg("num_productos"), 2).alias("promedio"),
)
stats_p8.show()
row8 = stats_p8.collect()[0]
resultados_texto.append(
    f"8. Productos por factura -> min: {row8['minimo']}, max: {row8['maximo']}, promedio: {row8['promedio']}"
)
guardar_csv(stats_p8, "08_stats_productos_por_factura")


# -----------------------------------------------------------------------
# Pregunta 9: Mes del año con más ventas
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 9: Mes con más ventas")

ventas_por_mes = (
    df.filter(F.col("Quantity") > 0)
    .groupBy("YearMonth")
    .agg(F.round(F.sum("Amount"), 2).alias("ventas_totales"))
    .orderBy(F.desc("ventas_totales"))
)
ventas_por_mes.show(5, truncate=False)
resultados_texto.append(
    f"9. Mes con más ventas: {ventas_por_mes.first()['YearMonth']} "
    f"(${ventas_por_mes.first()['ventas_totales']})"
)
guardar_csv(ventas_por_mes, "09_ventas_por_mes")


# -----------------------------------------------------------------------
# Pregunta 10: % de facturas con devoluciones (Quantity negativo)
# -----------------------------------------------------------------------
mostrar_titulo("Pregunta 10: % de facturas con devoluciones")

facturas_con_devolucion = (
    df.filter(F.col("EsDevolucion") == True)
    .select("InvoiceNo")
    .distinct()
    .count()
)
porcentaje_devoluciones = round((facturas_con_devolucion / total_facturas) * 100, 2)
print(f"Facturas con devolución: {facturas_con_devolucion} de {total_facturas} ({porcentaje_devoluciones}%)")
resultados_texto.append(
    f"10. % de facturas con devoluciones: {porcentaje_devoluciones}% "
    f"({facturas_con_devolucion} de {total_facturas})"
)

pd_p10 = spark.createDataFrame(
    [(facturas_con_devolucion, total_facturas, porcentaje_devoluciones)],
    ["facturas_con_devolucion", "total_facturas", "porcentaje"]
)
guardar_csv(pd_p10, "10_porcentaje_devoluciones")


# -----------------------------------------------------------------------
# EXTRA (opcional): Funciones de ventana - Ranking de clientes por país
# -----------------------------------------------------------------------
mostrar_titulo("EXTRA: Ranking de clientes por monto de compra (Window functions)")

ventas_cliente_pais = (
    df.filter((F.col("Quantity") > 0) & (F.col("CustomerID").isNotNull()))
    .groupBy("Country", "CustomerID")
    .agg(F.round(F.sum("Amount"), 2).alias("monto_total"))
)

ventana = Window.partitionBy("Country").orderBy(F.desc("monto_total"))

ranking_clientes = (
    ventas_cliente_pais
    .withColumn("ranking", F.row_number().over(ventana))
    .filter(F.col("ranking") <= 3)   # top 3 clientes por país
    .orderBy("Country", "ranking")
)
ranking_clientes.show(15, truncate=False)
guardar_csv(ranking_clientes, "11_ranking_clientes_por_pais")


# -----------------------------------------------------------------------
# 4. UNIONES (JOIN) - ejemplo de dimensión de productos
# -----------------------------------------------------------------------
mostrar_titulo("EXTRA: Ejemplo de JOIN entre DataFrames")

dim_productos = (
    df.select("StockCode", "Description")
    .distinct()
    .groupBy("StockCode")
    .agg(F.first("Description").alias("Description"))
)

ventas_join = (
    df.filter(F.col("Quantity") > 0)
    .groupBy("StockCode")
    .agg(F.sum("Amount").alias("monto_total"))
    .join(dim_productos, on="StockCode", how="left")
    .orderBy(F.desc("monto_total"))
)
ventas_join.show(5, truncate=False)
guardar_csv(ventas_join, "12_ventas_join_productos")


# -----------------------------------------------------------------------
# 5. RESUMEN FINAL EN CONSOLA
# -----------------------------------------------------------------------
mostrar_titulo("RESUMEN DE RESPUESTAS")
for linea in resultados_texto:
    print(linea)

# Guardamos también el resumen en un .txt para armar el documento de conclusiones
with open(os.path.join(OUTPUT_DIR, "resumen_respuestas.txt"), "w", encoding="utf-8") as f:
    f.write("RESUMEN DE RESPUESTAS - Taller ETL PySpark (Online Retail)\n")
    f.write("=" * 60 + "\n\n")
    for linea in resultados_texto:
        f.write(linea + "\n")

print(f"\nResumen guardado en {OUTPUT_DIR}/resumen_respuestas.txt")

spark.stop()
