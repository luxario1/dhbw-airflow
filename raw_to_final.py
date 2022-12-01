import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc
import pandas
import sqlite3

def get_args():
    pass
if __name__ == '__main__':
    con = sqlite3.connect("/home/airflow/xkcd.db") #verbinde die Datenbank
    #initialisieren der noetigen Spark-Requirements
    sparkContext = pyspark.SparkContext()
    session = SparkSession(sparkContext)
    #JSON-Daten rekursiv aus HDFS einlesen
    data = session.read.format('json')\
        .option('recursiveFileLookup',True)\
        .load('/user/hadoop/xkcd/raw/*')

    #Daten auf noetigste Informationen reduzieren
    xkcdObj = data.drop("alt").drop("day").drop("link").drop("month").drop("news").drop("transcript").drop("extra_parts").drop("title")

    #Daten als nach Jahr-partitionierte csv-Dateien in den final-Ordner in HDFS speichern und mit ihnen eine Tabelle erstellen
    xkcdObj.write.format('csv')\
        .mode('overwrite')\
        .option('path', '/user/hadoop/xkcd/final')\
        .partitionBy('year')\
        .saveAsTable('default.xkcd_partitioned')\

    df = xkcdObj.toPandas() #Daten in Dataframe speichern

    df.to_sql("xkcd", con, if_exists="replace") #Dataframe in eine Tabelle und in die Datenbank speichern
    con.close() #Datenbank-Verbindung schliessen
