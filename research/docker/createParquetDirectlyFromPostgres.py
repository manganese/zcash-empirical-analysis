'''
This program creates parquet files directly from postgres
'''

import helpfulFunctions as hf
import os
import config as conf

def main():
    print("Generated parquest files from csv")
    path = '/root/research/'
    sc, sql_sc = hf.initiateSpark()
    sparkClassPath = os.getenv('SPARK_CLASSPATH', '/opt/spark/postgresql-42.2.2.jar')
    sc._conf.set('spark.jars', 'file:%s' % sparkClassPath)
    sc._conf.set('spark.executor.extraClassPath', sparkClassPath)
    sc._conf.set('spark.driver.extraClassPath', sparkClassPath)
    url = 'postgresql://zcashpostgres:5432/zcashdb?user=postgres'
    properties = {"driver": 'com.postgresql.jdbc.Driver'}
    dft, dfb, dfc, dfvi, dfvo, dfvj = hf.readDataframeFromPostgres(sql_sc, url, properties, conf.blockheight)
    hf.saveDataframeToParquet(path, dft, dfb, dfc, dfvi, dfvo, dfvj)
    print("created parquet files")


if __name__ == '__main__':
    main()
