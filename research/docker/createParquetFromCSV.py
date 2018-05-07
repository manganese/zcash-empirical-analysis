'''
This program creates parquet files from the postgres generated zcash CSVs
'''

import helpfulFunctions as hf
import os
import config as conf

def main():
    print("Generated parquest files from csv")
    path = conf.pathresearch
    sc, sql_sc = hf.initiateSpark()
    t =  path + 'public.transaction.csv'
    b =  path + 'public.block.csv'
    cg = path + 'public.coingen.csv'
    vi = path + 'public.vin.csv'
    vo = path + 'public.vout.csv'
    vj = path + 'public.vjoinsplit.csv'
    sparkClassPath = os.getenv('SPARK_CLASSPATH', '/opt/spark/postgresql-42.2.2.jar')
    sc._conf.set('spark.jars', 'file:%s' % sparkClassPath)
    sc._conf.set('spark.executor.extraClassPath', sparkClassPath)
    sc._conf.set('spark.driver.extraClassPath', sparkClassPath)
    print("Reading Data from CSV files")
    t, b, c, vi, vo, vj = hf.getRdds(sc=sc, file_tx=t, file_blocks=b, file_coingen=cg, file_vin=vi, file_vout=vo,
                                     file_vjoinsplit=vj)
    print("Converting to dataframes")
    dft, dfb, dfc, dfvi, dfvo, dfvj = hf.convertToDataframes(sql_sc, t, b, c, vi, vo, vj, conf.blockheight)
    print("Saving data to disk")
    hf.saveDataframeToParquet(path, dft, dfb, dfc, dfvi, dfvo, dfvj)
    print("created parquet files")


if __name__ == '__main__':
    main()
