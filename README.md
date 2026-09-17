# Taller ETL con PySpark — Online Retail Dataset

Proyecto de ETL con PySpark que limpia transacciones de Online Retail, responde
las diez preguntas del taller y exporta resultados a CSV.

## Instalación

Probado con Python 3.14.7, Temurin JDK 17 y PySpark 4.2.0:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install pyspark==4.2.0 openpyxl
```

Si PowerShell bloquea la activación:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Compatibilidad de Hadoop en Windows

PySpark necesita `winutils.exe` para escribir los CSV en Windows. Ejecuta una
sola vez desde la raíz del proyecto:

```powershell
New-Item -ItemType Directory -Force .hadoop\bin
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile ".hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile ".hadoop\bin\hadoop.dll"
```

Este binario es una dependencia local y no se sube a GitHub.

## Dataset

Descarga [Online Retail.xlsx desde UCI](https://archive.ics.uci.edu/static/public/352/online+retail.zip),
descomprímelo y pon el archivo `Online Retail.xlsx` en `data/`.

Al ejecutar, el script convierte automáticamente el Excel a
`data/Online Retail.csv` en UTF-8. Si no hay Excel, usa el CSV existente,
que debe tener las columnas `InvoiceNo`, `StockCode`, `Description`,
`Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID` y `Country`.

## Ejecución

```powershell
.\venv\Scripts\python.exe etl_online_retail.py
```

La carpeta `output/` contendrá los diez CSV requeridos, dos resultados extra
y `resumen_respuestas.txt`.

## Entregable

Sube a GitHub el script, README, `.gitignore`, la carpeta `output/` y un breve
documento de conclusiones basado en `output/resumen_respuestas.txt`.
