'''
This script has some helpful functions to find information fast.
Copy and paste this code into the pyspark terminal with cpaste then 
run it. This does not work as a standalone script, but must be imported.
'''

import sys
import csv
import pickle
import os, shutil
import random, string
from pyspark import SparkConf, SQLContext, SparkContext
import pyspark.sql.types as sqlTypes
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import pyspark.sql.functions as func
from os import access, R_OK
from os.path import isfile
import itertools
import config as conf
from decimal import *
getcontext().prec = 8

schemas = {
    "transaction": sqlTypes.StructType([
        sqlTypes.StructField("t_tx_hash", sqlTypes.StringType(), True),
        # sqlTypes.StructField("t_version", sqlTypes.IntegerType(), True),
        # sqlTypes.StructField("t_locktime", sqlTypes.LongType(), True),
        sqlTypes.StructField("t_blockhash", sqlTypes.StringType(), True),
        sqlTypes.StructField("t_time", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_blocktime", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_valid", sqlTypes.StringType(), True),
        sqlTypes.StructField("t_tx_vin_count", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_tx_vj_count", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_tx_vout_count", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_type", sqlTypes.StringType(), True)
    ]),
    "block": sqlTypes.StructType([
            sqlTypes.StructField("b_height", sqlTypes.LongType(), True),
            sqlTypes.StructField("b_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("b_size", sqlTypes.IntegerType(), True),
            sqlTypes.StructField("b_version", sqlTypes.IntegerType(), True),
            # sqlTypes.StructField("b_merkleroot", sqlTypes.StringType(), True),
            sqlTypes.StructField("b_tx", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            sqlTypes.StructField("b_time", sqlTypes.IntegerType(), True),
            # sqlTypes.StructField("b_nonce", sqlTypes.IntegerType(), True),
            # sqlTypes.StructField("b_difficulty", sqlTypes.StringType(), True),
            # sqlTypes.StructField("b_chainwork", sqlTypes.StringType(), True),
            # sqlTypes.StructField("b_anchor", sqlTypes.StringType(), True),
            sqlTypes.StructField("b_previousblockhash", sqlTypes.StringType(), True),
            sqlTypes.StructField("b_nextblockhash", sqlTypes.StringType(), False)
        ]),
    "coingen": sqlTypes.StructType([
            # sqlTypes.StructField("c_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("c_tx_hash",sqlTypes.StringType(), True),
            sqlTypes.StructField("c_coinbase", sqlTypes.StringType(), True),
            sqlTypes.StructField("c_sequence", sqlTypes.LongType(), True)
    ]),
    "vin": sqlTypes.StructType([
            # sqlTypes.StructField("vi_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("vi_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vi_prevout_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vi_prevout_n", sqlTypes.LongType(), True),
            # sqlTypes.StructField("vi_sequence", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vi_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
    ]),
    "vout": sqlTypes.StructType([
            # sqlTypes.StructField("vo_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("vo_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vo_value", sqlTypes.DecimalType(32,8), True),
            # sqlTypes.StructField("vo_valueZat", sqlTypes.DoubleType(), True),
            sqlTypes.StructField("vo_tx_n", sqlTypes.LongType(), True),
            sqlTypes.StructField("vo_pubkey", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            # sqlTypes.StructField("vo_isVjoinsplit", sqlTypes.StringType(), True),
    ]),
    "vjoinsplit": sqlTypes.StructType([
            # sqlTypes.StructField("vj_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("vj_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_vpub_old", sqlTypes.DecimalType(32,8), True),
            sqlTypes.StructField("vj_vpub_new", sqlTypes.DecimalType(32,8), True),
            # sqlTypes.StructField("vj_anchor", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_nullifiers", sqlTypes.ArrayType(sqlTypes.StringType), True),
            # sqlTypes.StructField("vj_commitments", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_onetimePubKey", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_randomSeed", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_macs", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_source_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            sqlTypes.StructField("vj_dest_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
    ])
}


def randomword(length):
   return ''.join(random.choice(string.lowercase) for i in range(length))


def initiateSpark():
    '''
    create the configuration file and spark environment
    :return: SparkContext and SQLContext
    '''
    print("Initialising an instance of Apache Spark")
    conf = SparkConf()
    conf.setAll([
        ('spark.driver.maxResultSize', '20G'),
        ('spark.executor.memory', '15g'),
        ('spark.driver.memory', '15g'),
        ('spark.executor.cores', '8'),
        # ('spark.local.dir',  '/tmp/'+randomword(5)+'/')
    ]).setAppName("Zcash-research")\
        .setMaster("local[4]")\
        .set("spark.io.compression.codec", "snappy")\
        .set("spark.rdd.compress", "true")
    sc = SparkContext(conf=conf)
    sql_sc = SQLContext(sc)
    return sc, sql_sc


def cleanAndSplit(dirtyString):
    '''
    turns a unicode string into a cleaned array
    :param dirtyString:
    :return:
    '''
    if "Set(" in dirtyString:
        cleanArray = dirtyString.replace("Set(", "").replace(")", "").replace(" ", "").split(",")
    else:
        cleanArray = dirtyString.replace("{", "").replace("}","").replace(" ", "").replace("[","").replace("]","").split(",")
    if len(cleanArray) == 1 and cleanArray[0] == u'' or len(cleanArray)==0:
        return []
    cleanArray = [str(x) for x in cleanArray]
    return cleanArray


def checkFilesExist(files):
    for filex in files:
        if not isfile(filex) or not access(filex, R_OK):
            print("File %s doesn't exist or isn't readable" % filex)
            sys.exit(1)


def getRdds(sc, file_tx, file_blocks, file_coingen, file_vin, file_vout, file_vjoinsplit):
    '''
    Returns a parsed and cleaned set of RDDs
    taken from gkappos' spark code
    :return: many items
    '''
    # if files don't exist and I can't read them then quit
    checkFilesExist([file_tx, file_blocks, file_coingen, file_vin, file_vout, file_vjoinsplit])

    tx = sc.textFile(file_tx)
    blocks = sc.textFile(file_blocks)
    coingen = sc.textFile(file_coingen)
    vin = sc.textFile(file_vin)
    vout = sc.textFile(file_vout)
    vjoinsplit =sc.textFile(file_vjoinsplit)

    # Split them.
    tx = tx.map(lambda x: x.split(';'))
    blocks = blocks.map(lambda x: x.split(';'))
    coingen = coingen.map(lambda x: x.split(';'))
    vin = vin.map(lambda x: x.split(';'))
    vout = vout.map(lambda x: x.split(';'))
    vjoinsplit = vjoinsplit.map(lambda x: x.split(';'))

    # remove headers
    tx = tx.filter(lambda x: x[0] != u'tx_hash')
    # filter out genesis transaction, its buggy
    tx = tx.filter(lambda x: x[0] != u'c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb')
    blocks = blocks.filter(lambda x: x[0] != u'height')
    coingen = coingen.filter(lambda x: x[0] != u'db_id')
    vin = vin.filter(lambda x: x[0] != u'db_id')
    vout = vout.filter(lambda x: x[0] != u'db_id')
    vjoinsplit = vjoinsplit.filter(lambda x: x[0] != u'db_id')

    # clean them
    tx = tx.map(lambda x: [
        x[0],
        int(x[1]),
        long(x[2]),
        x[3],
        int(x[4]),
        int(x[5]),
        x[6],
        int(x[7]),
        int(x[8]),
        int(x[9]),
        x[10]
    ])

    blocks = blocks.map(lambda x: [
        long(x[0]),     # "b_height",
        str(x[1]),      # "b_hash",
        int(x[2]),      # "b_size",
        int(x[3]),      # "b_version",
        str(x[4]),    # "b_merkleroot",
        cleanAndSplit(x[5]),           # "b_tx",
        int(x[6]),      # "b_time",
        str(x[7]),     # "b_nonce",
        str(x[8]),     # "b_difficulty",
        str(x[9]),     # "b_chainwork",
        str(x[10]),    # "b_anchor",
        str(x[11]),     # "b_previousblockhash",
        str(x[12]),     # "b_nextblockhash",
    ])

    coingen = coingen.map(lambda x: [
        str(x[0]),     # c_db_id
        str(x[1]),      # c_tx_hash
        str(x[2]),      # c_coinbase
        long(x[3])      # c_sequence
    ])

    vin = vin.map(lambda x: [
        str(x[0]),       # vi_db_id
        str(x[1]),          # vi_tx_hash
        str(x[2]),          # vi_prevout_hash
        int(x[3]),         # vi_prevout_n
        str(x[4])       # vi_sequence
        # cleanAndSplit(x[5]), # vi_address
    ])
    vout = vout.map(lambda x: [
        x[0],           #db_id;
        x[1],           #tx_hash;
        Decimal(x[2]),            #value;
        x[3],           #valueZat;
        x[4],           #tx_n;
        cleanAndSplit(x[5]),            #pubkey;
        x[6]            #isVjoinsplit
    ])

    vjoinsplit = vjoinsplit.map(lambda x: [
        x[0],
        x[1],
        Decimal(x[2]),
        Decimal(x[3]),
        x[4],
        cleanAndSplit(x[5]),
        cleanAndSplit(x[6]),
        cleanAndSplit(x[7]),
        x[8],
        cleanAndSplit(x[9]),
        cleanAndSplit(x[10]),
        cleanAndSplit(x[11])
    ])
    return tx, blocks, coingen, vin, vout, vjoinsplit


def readDataframeFromPostgres(sql_sc, url, properties, blockheight):
    '''
    This method reads the dataframes directly from postgres.
    It returns smaller sized dataframes up to the blocklimit.
    :param sc:
    :param sql_sc:
    :param blocklimit: the number of blocks to process up to
    :param url: url to postgres server
    :param properties: dictionary of properties for postgres
    :return:
    '''
    # Blocks
    # height;hash;size;version;merkleroot;tx;time;nonce;difficulty;chainwork;anchor;previousblockhash;nextblockhash
    # 0;00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08;1692;4;c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb;{c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb};1477641360;0000000000000000000000000000000000000000000000000000000000001257;1;0000000000000000000000000000000000000000000000000000000000002000;59d2cde5e65c1414c32ba54f0fe4bdb3d67618125286e6a191317917c812c6d7;"";0007bc227e1c57a4a70e237cad00e7b7ce565155ab49166bc57397a26d339283
    print("Generating parquets up to block %i" % (blockheight))
    dfb = sql_sc.read.format('jdbc').options(url='jdbc:%s' % url, dbtable='blocks', properties=properties).load()
    dfb = dfb.drop('merkleroot', 'nonce', 'difficulty', 'chainwork', 'anchor')\
        .withColumnRenamed('height', 'b_height')\
        .withColumnRenamed('hash', 'b_hash')\
        .withColumnRenamed('size', 'b_size')\
        .withColumnRenamed('version', 'b_version')\
        .withColumnRenamed('tx', 'b_tx')\
        .withColumnRenamed('time', 'b_time')\
        .withColumnRenamed('previousblockhash', 'b_previousblockhash')\
        .withColumnRenamed('nextblockhash', 'b_nextblockhash')
    dfb = dfb.where(dfb.b_height <= blockheight)
    dfb_hash = dfb.select(dfb.b_hash)

    # Transactions
    dft = sql_sc.read.format('jdbc').options(url='jdbc:%s' % url, dbtable='transactions', properties=properties).load()
    dft = dft.drop('version', 'locktime')\
        .withColumnRenamed('tx_hash', 't_tx_hash')\
        .withColumnRenamed('blockhash', 't_blockhash')\
        .withColumnRenamed('time', 't_time')\
        .withColumnRenamed('blocktime', 't_blocktime')\
        .withColumnRenamed('valid', 't_valid')\
        .withColumnRenamed('tx_vin_count', 't_tx_vin_count')\
        .withColumnRenamed('tx_vj_count', 't_tx_vj_count')\
        .withColumnRenamed('tx_vout_count', 't_tx_vout_count')\
        .withColumnRenamed('type', 't_type')
    # only get transactions from blockheights we are limited to
    dft = dft.join(dfb_hash, dft.t_blockhash == dfb_hash.b_hash, 'inner').drop('b_hash')
    dft_hash = dft.select("t_tx_hash")

    # Coingens
    dfc = sql_sc.read.format('jdbc').options(url='jdbc:%s' % url, dbtable='coingen', properties=properties).load()
    dfc = dfc.drop('db_id').withColumnRenamed('tx_hash', 'c_tx_hash')\
        .withColumnRenamed('coinbase', 'c_coinbase')\
        .withColumnRenamed('sequence', 'c_sequence')
    dfc = dfc.join(dft_hash, dfc.c_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')

    #  Vouts
    # db_id;tx_hash;;;;;
    dfvo = sql_sc.read.format('jdbc').options(url='jdbc:%s' % url, dbtable='vout', properties=properties).load()
    dfvo = dfvo.drop('db_id', 'isVjoinsplit', 'valueZat')\
        .withColumnRenamed('tx_hash', 'vo_tx_hash')\
        .withColumnRenamed('value', 'vo_value')\
        .withColumnRenamed('tx_n', 'vo_tx_n')\
        .withColumnRenamed('pubkey', 'vo_pubkey')
    dfvo = dfvo.join(dft_hash, dfvo.vo_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')

    # Vinputs
    dfvi = sql_sc.read.format('jdbc').options(url='jdbc:%s' % url, dbtable='vin', properties=properties).load()
    dfvi = dfvi.drop('db_id', 'sequence', 'address')\
        .withColumnRenamed('tx_hash', 'vi_tx_hash')\
        .withColumnRenamed('prevout_hash', 'vi_prevout_hash')\
        .withColumnRenamed('prevout_n', 'vi_prevout_n')
    dfvi = dfvi.join(dft_hash, dfvi.vi_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')
    # join with the vout to obtain the address
    dfvivo = dfvi.join(dfvo, (dfvi.vi_prevout_hash == dfvo.vo_tx_hash) & (dfvi.vi_prevout_n == dfvo.vo_tx_n))
    # drop the vout columns
    dfvi = dfvivo.drop('vo_tx_hash', 'vo_tx_n', 'vo_value')
    # rename vo pubkey to vi address
    dfvi = dfvi.withColumnRenamed('vo_pubkey', 'vi_address')

    #Vjoinsplits
    # ;;;;;;;;;;;
    dfvj = sql_sc.read.format('jdbc').options(url='jdbc:%s' % url, dbtable='vjoinsplit', properties=properties).load()
    dfvj = dfvj.drop('db_id', 'anchor', 'nullifiers', 'commitments', 'onetimePubKey', 'randomSeed', 'macs')\
        .withColumnRenamed('tx_hash', 'vj_tx_hash')\
        .withColumnRenamed('vpub_old', 'vj_vpub_old')\
        .withColumnRenamed('vpub_new', 'vj_vpub_new')\
        .withColumnRenamed('source_address', 'vj_source_address')\
        .withColumnRenamed('dest_address', 'vj_dest_address')
    dfvj = dfvj.join(dft_hash, dfvj.vj_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')
    dfvj = dfvj.withColumn("vj_vpub_new", func.round(dfvj["vj_vpub_new"], 8))
    dfvj = dfvj.withColumn("vj_vpub_old", func.round(dfvj["vj_vpub_old"], 8))
    dfvj = getSourceDestinationAddress(sql_sc, dfvi, dfvo, dfvj)

    print "vin"
    dfvi.write.parquet("vin.parquet")
    print "vout"
    dfvo.write.parquet("vout.parquet")
    print "vjoin"
    dfvj.write.parquet("vjoinsplit.parquet")

    return dft, dfb, dfc, dfvi, dfvo, dfvj


def createVinAddresses(virdd, vordd):
    vinskv = virdd.map(lambda x: ((x[2], x[3]), x[0:4]))
    voutskv = vordd.map(lambda x: ((x[1], x[4]), x[5]))
    vinvout = vinskv.join(voutskv)
    print(vinskv.count())
    print(vinvout.count())
    # copy addresses to vin
    vin = vinvout.map(lambda x: x[:7])
    return vin


def mergeLists(x):
    full = []
    for a in x:
            full.extend(a)
    return list(set(full))


def getSourceDestinationAddress(sql_sc, dfvi, dfvo, dfvj):
    # Get the Vins, Vouts for the
    dfvirdd = dfvi.select("vi_tx_hash", "vi_address").rdd
    dfvirddgbk = dfvirdd.map(lambda row: (row['vi_tx_hash'], row['vi_address'])).groupByKey()
    dfvirddgbkvals = dfvirddgbk.map(lambda x: (x[0], mergeLists(x[1])))
    dfviaddr = sql_sc.createDataFrame(dfvirddgbkvals)
    dfviaddr = dfviaddr.withColumnRenamed('_1', 'vi_tx_hash').withColumnRenamed('_2', 'vi_address')
    dfvirdd.unpersist()
    dfvirddgbk.unpersist()

    dfvordd = dfvo.select("vo_tx_hash", "vo_pubkey").rdd
    dfvorddgbk = dfvordd.map(lambda row: (row['vo_tx_hash'], row['vo_pubkey'])).groupByKey()
    dfvorddgbkvals = dfvorddgbk.map(lambda x: (x[0], mergeLists(x[1])))
    dfvoaddr = sql_sc.createDataFrame(dfvorddgbkvals)
    dfvoaddr = dfvoaddr.withColumnRenamed('_1', 'vo_tx_hash').withColumnRenamed('_2', 'vo_pubkey')
    dfvirdd.unpersist()
    dfvorddgbk.unpersist()

    print(dfvj.count())
    dfvj = dfvj.join(dfviaddr, (dfvj.vj_tx_hash == dfviaddr.vi_tx_hash), 'left')
    dfvj = dfvj.join(dfvoaddr, (dfvj.vj_tx_hash == dfvoaddr.vo_tx_hash), 'left')
    dfvj = dfvj.drop('vi_tx_hash', 'vo_tx_hash', 'vj_source_address', 'vj_dest_address')\
        .withColumnRenamed('vi_address','vj_source_address')\
        .withColumnRenamed('vo_pubkey', 'vj_dest_address')
    print(dfvj.count())
    return dfvj


def createEmptyDataframes(sc, sql_sc):
    '''
    returns empty datafra,es
    :return:
    '''
    # Transactions
    # tx_hash;version;locktime;blockhash;time;blocktime;valid;tx_vin_count;tx_vj_count;tx_vout_count;type
    dft = sql_sc.createDataFrame(sc.emptyRDD(), schemas['transaction'])

    # Blocks
    # height;hash;size;version;merkleroot;tx;time;nonce;difficulty;chainwork;anchor;previousblockhash;nextblockhash
    # 0;00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08;1692;4;c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb;{c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb};1477641360;0000000000000000000000000000000000000000000000000000000000001257;1;0000000000000000000000000000000000000000000000000000000000002000;59d2cde5e65c1414c32ba54f0fe4bdb3d67618125286e6a191317917c812c6d7;"";0007bc227e1c57a4a70e237cad00e7b7ce565155ab49166bc57397a26d339283
    dfb = sql_sc.createDataFrame(sc.emptyRDD(), schemas['block'])

    # Coingens
    dfc = sql_sc.createDataFrame(sc.emptyRDD(), schemas['coingen'])

    #  Vouts
    # db_id;tx_hash;;;;;
    dfvo = sql_sc.createDataFrame(sc.emptyRDD(), schemas['vout'])

    # Vinputs
    dfvi = sql_sc.createDataFrame(sc.emptyRDD(), schemas['vin'])

    #Vjoinsplits
    # ;;;;;;;;;;;
    dfvj = sql_sc.createDataFrame(sc.emptyRDD(), schemas['vjoinsplit'])

    return dft, dfb, dfc, dfvi, dfvo, dfvj


def populateDataframe(sql_sc, objects, dft, dfb, dfc, dfvi, dfvo, dfvj):
    '''
    Given a dictionary of arrays, add them to the dataframe
    :param objects:
    :param dft:
    :param dfb:
    :param dfc:
    :param dfvi:
    :param dfvo:
    :param dfvj:
    :return: the modified dataframes
    '''
    temp = sql_sc.createDataFrame(objects['transaction'], schema=schemas['transaction'])
    dft = dft.union(temp)

    temp = sql_sc.createDataFrame(objects['block'], schema=schemas['block'])
    dfb = dfb.union(temp)

    temp = sql_sc.createDataFrame(objects['coingen'], schema=schemas['coingen'])
    dfc = dfc.union(temp)

    temp = sql_sc.createDataFrame(objects['vin'], schema=schemas['vin'])
    dfvi = dfvi.union(temp)

    temp = sql_sc.createDataFrame(objects['vout'], schema=schemas['vout'])
    dfvo = dfvo.union(temp)

    temp = sql_sc.createDataFrame(objects['vjoinsplit'], schema=schemas['vjoinsplit'])
    dfvj = dfvj.union(temp)

    return dft, dfb, dfc, dfvi, dfvo, dfvj


def convertToDataframes(sql_sc, t, b, c, vi, vo, vj, blockheight):
    # Blocks
    # height;hash;size;version;merkleroot;tx;time;nonce;difficulty;chainwork;anchor;previousblockhash;nextblockhash
    # 0;00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08;1692;4;c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb;{c4eaa58879081de3c24a7b117ed2b28300e7ec4c4c1dff1d3f1268b7857a4ddb};1477641360;0000000000000000000000000000000000000000000000000000000000001257;1;0000000000000000000000000000000000000000000000000000000000002000;59d2cde5e65c1414c32ba54f0fe4bdb3d67618125286e6a191317917c812c6d7;"";0007bc227e1c57a4a70e237cad00e7b7ce565155ab49166bc57397a26d339283
    b = b.map(lambda x: [x[0],x[1],x[2],x[3],x[5],x[6],x[11],x[12]])
    blockSchema = sqlTypes.StructType([
        sqlTypes.StructField("b_height", sqlTypes.LongType(), True),
        sqlTypes.StructField("b_hash", sqlTypes.StringType(), True),
        sqlTypes.StructField("b_size", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("b_version", sqlTypes.IntegerType(), True),
        # sqlTypes.StructField("b_merkleroot", sqlTypes.StringType(), True),
        sqlTypes.StructField("b_tx", sqlTypes.ArrayType(sqlTypes.StringType()), True),
        sqlTypes.StructField("b_time", sqlTypes.IntegerType(), True),
        # sqlTypes.StructField("b_nonce", sqlTypes.IntegerType(), True),
        # sqlTypes.StructField("b_difficulty", sqlTypes.StringType(), True),
        # sqlTypes.StructField("b_chainwork", sqlTypes.StringType(), True),
        # sqlTypes.StructField("b_anchor", sqlTypes.StringType(), True),
        sqlTypes.StructField("b_previousblockhash", sqlTypes.StringType(), True),
        sqlTypes.StructField("b_nextblockhash", sqlTypes.StringType(), False)
    ])
    dfb = sql_sc.createDataFrame(b, blockSchema)
    dfb = dfb.where(dfb.b_height <= blockheight)
    dfb_hash = dfb.select(dfb.b_hash)
    b.unpersist()

    # Transactions
    # tx_hash;version;locktime;blockhash;time;blocktime;valid;tx_vin_count;tx_vj_count;tx_vout_count;type
    t = t.map(lambda x: [x[0], x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10]])
    txSchema = sqlTypes.StructType([
        sqlTypes.StructField("t_tx_hash", sqlTypes.StringType(), True),
        # sqlTypes.StructField("t_version", sqlTypes.IntegerType(), True),
        # sqlTypes.StructField("t_locktime", sqlTypes.LongType(), True),
        sqlTypes.StructField("t_blockhash", sqlTypes.StringType(), True),
        sqlTypes.StructField("t_time", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_blocktime", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_valid", sqlTypes.StringType(), True),
        sqlTypes.StructField("t_tx_vin_count", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_tx_vj_count", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_tx_vout_count", sqlTypes.IntegerType(), True),
        sqlTypes.StructField("t_type", sqlTypes.StringType(), True)
    ])
    dft = sql_sc.createDataFrame(t, txSchema)
    dft = dft.join(dfb_hash, dft.t_blockhash == dfb_hash.b_hash, 'inner').drop('b_hash')
    dft_hash = dft.select("t_tx_hash")
    t.unpersist()

    # Coingens
    c = c.map(lambda x: [x[1], x[2], x[3]])
    coingenSchema = sqlTypes.StructType(
        [
            # sqlTypes.StructField("c_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("c_tx_hash",sqlTypes.StringType(), True),
            sqlTypes.StructField("c_coinbase", sqlTypes.StringType(), True),
            sqlTypes.StructField("c_sequence", sqlTypes.LongType(), True)
    ])
    dfc = sql_sc.createDataFrame(c, coingenSchema)
    dfc = dfc.join(dft_hash, dfc.c_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')
    c.unpersist()

    #  Vouts
    # db_id;tx_hash;;;;;
    vo = vo.map(lambda x: [x[1], x[2], int(x[4]), x[5]])
    voSchema = sqlTypes.StructType(
        [
            # sqlTypes.StructField("vo_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("vo_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vo_value", sqlTypes.DecimalType(32,8), True),
            # sqlTypes.StructField("vo_valueZat", sqlTypes.DoubleType(), True),
            sqlTypes.StructField("vo_tx_n", sqlTypes.LongType(), True),
            sqlTypes.StructField("vo_pubkey", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            # sqlTypes.StructField("vo_isVjoinsplit", sqlTypes.StringType(), True),
    ])
    dfvo = sql_sc.createDataFrame(vo, voSchema)
    # dfvo = dfvo.withColumn("vo_value", func.round(dfvo["vo_value"], 8))
    dfvo = dfvo.join(dft_hash, dfvo.vo_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')
    vo.unpersist()

    # Vinputs
    vix = vi.map(lambda x: [x[1], x[2], x[3]])
    vinSchema = sqlTypes.StructType(
        [
            # sqlTypes.StructField("vi_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("vi_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vi_prevout_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vi_prevout_n", sqlTypes.LongType(), True),
            # sqlTypes.StructField("vi_sequence", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vi_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
    ])
    dfvi = sql_sc.createDataFrame(vix, vinSchema)
    dfvi = dfvi.join(dft_hash, dfvi.vi_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')
    vix.unpersist()
    vi.unpersist()
    # join with the vout to obtain the address
    dfvivo = dfvi.join(dfvo, (dfvi.vi_prevout_hash == dfvo.vo_tx_hash) & (dfvi.vi_prevout_n == dfvo.vo_tx_n))
    # drop the vout columns
    dfvi = dfvivo.drop('vo_tx_hash', 'vo_tx_n', 'vo_value')
    # rename vo pubkey to vi address
    dfvi = dfvi.withColumnRenamed('vo_pubkey', 'vi_address')

    #Vjoinsplits
    # ;;;;;;;;;;;
    vj = vj.map(lambda x: [x[1], x[2], x[3], x[10], x[11]])
    vjSchema = sqlTypes.StructType(
        [
            # sqlTypes.StructField("vj_db_id", sqlTypes.LongType(), True),
            sqlTypes.StructField("vj_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_vpub_old", sqlTypes.DecimalType(32,8), True),
            sqlTypes.StructField("vj_vpub_new", sqlTypes.DecimalType(32,8), True),
            # sqlTypes.StructField("vj_anchor", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_nullifiers", sqlTypes.ArrayType(sqlTypes.StringType), True),
            # sqlTypes.StructField("vj_commitments", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_onetimePubKey", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_randomSeed", sqlTypes.StringType(), True),
            # sqlTypes.StructField("vj_macs", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_source_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            sqlTypes.StructField("vj_dest_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
    ])
    dfvj = sql_sc.createDataFrame(vj, vjSchema)
    dfvj = dfvj.join(dft_hash, dfvj.vj_tx_hash == dft_hash.t_tx_hash, 'inner').drop('t_tx_hash')
    # dfvj = dfvj.withColumn("vj_vpub_old", func.round(dfvj["vj_vpub_old"], 8))
    # dfvj = dfvj.withColumn("vj_vpub_new", func.round(dfvj["vj_vpub_new"], 8))
    dfvj = getSourceDestinationAddress(sql_sc, dfvi, dfvo, dfvj)
    vj.unpersist()
    return dft, dfb, dfc, dfvi, dfvo, dfvj


def saveDataframeToParquet(dirx, dft, dfb, dfc, dfvi, dfvo, dfvj):
    # if old parquet files exist delete them
    files = {
        "transaction.parquet": dft,
        "block.parquet": dfb,
        "coingen.parquet": dfc,
        "vin.parquet": dfvi,
        "vout.parquet": dfvo,
        "vjoinsplit.parquet": dfvj,
    }
    for k,v in files.iteritems():
        if os.path.exists(dirx+k):
            print("Deleting existing %s" % (k))
            shutil.rmtree(dirx+k)
        print("Saving %s/%s" % (dirx,k))
        v.write.parquet(dirx+k)


def loadDataframesFromParquet(sql_sc, path):
    print "Loading Parquet files from folder %s" % path
    print "Loading transactions from %stransaction.parquet" % path
    dft = sql_sc.read.parquet(path + "transaction.parquet")
    print "Loading Blocks from %sblock.parquet" % path
    dfb = sql_sc.read.parquet(path + "block.parquet")
    print "Loading Coingens from %scoingen.parquet" % path
    dfc = sql_sc.read.parquet(path + "coingen.parquet")
    print "Loading Vins from %svin.parquet" % path
    dfvi = sql_sc.read.parquet(path + "vin.parquet")
    print "Loading Vouts from %svout.parquet" % path
    dfvo = sql_sc.read.parquet(path + "vout.parquet")
    print "Loading Vjoinsplits from %svjoinsplit.parquet" % path
    dfvj = sql_sc.read.parquet(path + "vjoinsplit.parquet")
    print "Files loaded"
    return dft, dfb, dfc, dfvi, dfvo, dfvj


def loadAddressesFromCsv(path=conf.pathresearch):
    '''
    This program loads the following csv file of addresses into dictionaries.
        - pool_addresses.csv
            single double column csv file with header 'address;tag'
            where each address had a single string as tag, for example 't13980obkl08819;starpool'
            tags can be repeated
            in this line, the address 't13980obkl08819' has a single tag called 'starpool'
            e.g. this csv would look like
                address;tag
                t13980obkl08819;starpool
        - address_tags.csv
            single double column csv file with header 'address;tag'
            where each address had a single string as tag, for example 't13980obkl08819;exchangeX'
            in this line, the address 't13980obkl08819' has a single tag called 'exchangeX'
            tags can be repeated
            e.g. this csv would look like
                address;tag
                t13980obkl08819;exchangeX
        - founders_addresses.csv
            single column csv file with header 'address'
            each address is on a separate line
            these addresses are the founders addresses
        - miners_addresses.csv
            single column csv file with header 'address'
            each address is on a separate line
            these addresses are the miners addresses
        - miners_heuristic_addresses.csv
            single column csv file with header 'address'
            each address is on a separate line
            these addresses are the miners addresses from the mining heuristic,
        - founders_heuristic_addresses.csv
            single column csv file with the header 'address'
            each address is on a separate line
            these addresses are the founders addresses from the founders heuristic

    '''
    # First check if the files exist
    # load the files into memory
    # convert them to sets and dictionaries
    # Return
    addressFiles = {
        "poolAddresses": path+'pool_addresses.csv',
        "addressTags" : path+'address_tags.csv',
        "foundersAddress" : path+'founders_addresses.csv',
        "minersAddresses" : path+'miners_addresses.csv',
        "minersHeuristicAddresses" : path+'miners_heuristic_addresses.csv',
        "foundersHeuristicAddresses" : path+'founders_heuristic_addresses.csv',
    }

    addressValues = {}

    for k,v in addressFiles.iteritems():
        if isfile(v):
            with open(v, 'r') as csvfile:
                mydict = []
                columnreader, rowreader = itertools.tee(csv.reader(csvfile, delimiter=';'))
                # check number of rows
                columns = len(next(columnreader))
                del columnreader
                # skip the header
                next(rowreader, None)
                try:
                    if columns == 0:
                        continue
                    elif columns == 1:
                        for row in rowreader:
                            if len(row):
                                mydict.append(row[0])
                        mydict = set(mydict)
                    elif columns == 2:
                        mydict = {}
                        for row in rowreader:
                            if len(row) == 2:
                                mydict[row[0]] = row[1]
                    else:
                        print("%s (%s) has unexpected row size" % (k,v))
                        print("Not expecting anything more than 2")
                        print("exiting")
                        sys.exit(1)
                except IndexError as e:
                    print e
                    print("Index error with %s file" % (v))
                    print("Exiting")
                    sys.exit(1)
                addressValues[k] = mydict
        else:
            print("File %s not found in %s" % (k,v))
            print("Exiting")
            sys.exit(1)

    return addressValues


def sumFromResultIterable(resIter, index):
    total = 0.0
    if type(resIter) != list:
        # print "converting resiter to list"
        # print type(resIter)
        # print resIter
        resIter = list(resIter)
    for val in resIter:
        total += val[index]
    return total


def getTotalTxSent(transactionsSent, dfvo):
    '''
    Given a set of transactions, it finds the total sent!
    I apolgise for the bad variable names
    :return:
    '''
    qt = transactionsSent.map(lambda x: list(x[1]))
    qt2 = qt.flatMap(lambda x: x)
    qt3 = qt2.map(lambda x: ((x[2], x[3]), 1))
    qt4 = qt3.join(dfvo.map(lambda x: ((x[1], x[4]), x[2])))
    tot = qt4.map(lambda x: x[1][1]).sum()
    return tot


def getTotalTxSentDataframe(transactionsSent, dfvo):
    '''
    Given a set of transactions, it finds the total sent!
    I apolgise for the bad variable names
    :return:
    '''
    transactionSentprevhashprevn = transactionsSent.select(["vi_prevout_hash", "vi_prevout_n"])
    cond = (transactionSentprevhashprevn.vi_prevout_hash == dfvo.vo_tx_hash) & (transactionSentprevhashprevn.vi_prevout_n == dfvo.vo_tx_n)
    qt4 = transactionSentprevhashprevn.join(dfvo, cond)
    res = qt4.agg({"vo_value": "sum"}).collect()[0][0]
    return res


def getSrcAddresses(transactionsSent):
    '''
    Returns a list of src addresses from the transaction set given
    :param transactionsSent:
    :return:
    '''
    vins = transactionsSent.filter(lambda x: list(x[1]))
    flatvins = vins.flatMap(lambda x:x)
    addrs = flatvins.map(lambda x: x[-1])
    addrsx = addrs.flatMap(lambda x: list(x))
    return list(set(addrsx.collect()))


def getSingleAddressStats(vi, vo, vjx,maxBlockHeight, minBlockHeught,addressToFind):
    '''
    Given an ZEC address print and return the following stats
    * = done, ? = Not done
        * From what block numbers
        * Total Number of TXs Sent
        * Total Number of TXs received
        * Type of address - If founder or miner
        * How many Coingens its done (number of blocks mined)
        * Total ZEC put into the pool
        * Total ZEC taken from the pool
        * Blocks mined
        * Total transparent value
        * Current Balance
        * Other addresses it has been a source with (mandem)
        ? Balance over time
        ? Active times (first tx to last tx basically trivial)
    :param addressToFind:
    :return: Dictionary
    '''
    transactionsSent = vi.filter(lambda x: addressToFind in x[-1]).cache()
    transactionsSentCount = transactionsSent.count()
    if transactionsSentCount != 0:
        transactionsSent = transactionsSent.map(lambda x: (x[1], x))
        transactionsSent = transactionsSent.groupByKey()
        # transactionsSentList = transactionsSent.collect()
        transactionsSentCount = transactionsSent.count()
        totalSent = getTotalTxSent(transactionsSent, vo)
        vjoinsIn = transactionsSent.join(vjx)
        vjoinsInCount = vjoinsIn.count()
        # src_addrs = getSrcAddresses(transactionsSent)
        totalShielded = vjoinsIn.map(lambda x: x[1][1][2]).sum()
        if vjoinsIn.count() != vjoinsIn.filter(lambda x: x[1][1][2] > 0).count():
            print "error with vjoinin so don't believe the sum or count"
    else:
        totalSent = 0
        vjoinsInCount = 0
        # src_addrs = []
        totalShielded = 0
        # transactionsSentList = []
    src_addrs = []
    transactionsRecv = vo.filter(lambda x: addressToFind in x[-2])
    transactionsRecvCount = transactionsRecv.count()

    if transactionsRecvCount != 0:
        transactionsRecv = transactionsRecv.map(lambda x: (x[1], x))
        transactionsRecv = transactionsRecv.groupByKey()
        # transactionsRecvList = transactionsRecv.collect()
        transactionsRecvCount = transactionsRecv.count()
        totalRecv = transactionsRecv.map(lambda x: sumFromResultIterable(x[1], 2)).sum()
        coingens = transactionsRecv.join(c.map(lambda x: (x[1], x)))
        coingensCount = coingens.count()
        # not equal to founds reward
        if coingensCount != 0:
            blocksMined = coingens.filter(lambda x: sumFromResultIterable(x[1][0], 2) != 2.5)
            blocksMinedCount = blocksMined.count()
        else:
            coingensCount = 0

        vjoinsOut = transactionsRecv.join(vjx)
        vjoinsOutCount = vjoinsOut.count()
        if vjoinsOutCount != 0:
            totalDeshielded = vjoinsOut.map(lambda x: x[1][1][3]).sum()
        else:
            totalDeshielded = 0

        if vjoinsOutCount != vjoinsOut.filter(lambda x: x[1][1][3] > 0).count():
            print "error with vjoinout so don't believe the sum or count"
    else:
        totalRecv = 0
        coingensCount = 0
        blocksMinedCount = 0
        vjoinsOutCount = 0
        totalDeshielded = 0
        # transactionsRecvList = []

    # addressType = getAddressType(addressToFind)
    stats = {
        "address": addressToFind,
        "TX_sent":str(transactionsSentCount),
        "TX_recieved": str(transactionsRecvCount),
        # "Address_type":addressType,
        "Coingens":str(coingensCount),
        "Blocks mined" :str(blocksMinedCount),
        "Vjoins In" :str(vjoinsInCount),
        "Total shielded": str(totalShielded),
        "Vjoins Out" :str(vjoinsOutCount),
        "Total_deshielded": str(totalDeshielded),
        "Total_received: ": str(totalRecv),
        "Total_sent" : str(totalSent),
        "ZEC_Remaining" : str(totalRecv - totalSent),
        "Other_src_addr" : str(list(src_addrs))
    }
    print("Analysing    : " + str(addressToFind))
    print("From block   : " + str(minBlockHeught) + " to " + str(maxBlockHeight))
    print("Active times : TODO")
    print("TX sent      : " + str(transactionsSentCount))
    print("TX recieved  : " + str(transactionsRecvCount))
    # print("Address type : " + addressType)
    print("Coingens     : " + str(coingensCount))
    print("Blocks mined : " + str(blocksMinedCount))
    print("Vjoins In    : " + str(vjoinsInCount))
    print("Total shielded:" + str(totalShielded))
    print("Vjoins Out   : " + str(vjoinsOutCount))
    print("Total deshielded: "+str(totalDeshielded))
    print("Total received: " + str(totalRecv))
    print("Total sent    : " + str(totalSent))
    print("ZEC Remaining : " + str(totalRecv-totalSent))
    print("Other src addr: " + str(list(src_addrs)))
    return stats


def extractAddr2(row):
    if row.vi_address is not None and row.vo_pubkey is not None and row.vi_address == row.vo_pubkey:
        return row.vi_address[0]
    elif row.vi_address is not None and row.vo_pubkey is None:
        return row.vi_address[0]
    elif row.vi_address is None and row.vo_pubkey is not None:
        if len(row.vo_pubkey) == 0:
            return ""
        else:
            return row.vo_pubkey[0]


def ex(r):
    if len(r) == 0:
        return [""]
    else:
        return r[0]


def flatudf(x):
    if type(x) == list:
        if len(x) == 1:
            return x[0]
        else:
            return ''
    else:
        return x


def getInputAddresses(dfvi, dfvo):
    '''
    Gets input address for the vinputs
    :param dfvi:
    :param dfvo:
    :return:
    '''
    # join with the vout to obtain the address
    dfvivo = dfvi.join(dfvo, (dfvi.vi_prevout_hash == dfvo.vo_tx_hash) & (dfvi.vi_prevout_n == dfvo.vo_tx_n))
    # drop the vout columns
    dfvi = dfvivo.drop('vo_tx_hash', 'vo_tx_n', 'vo_value')
    # rename vo pubkey to vi address
    dfvi = dfvi.withColumnRenamed('vo_pubkey', 'vi_address')
    return dfvi


def generateAllStats(sc, dfvi, dfvo, dfc, dft, dfvj, path):
    # sent addresses
    print("Calculating sent values")
    joined = dfvi.join(dfvo, (dfvi.vi_prevout_hash == dfvo.vo_tx_hash) & (dfvi.vi_prevout_n == dfvo.vo_tx_n), "leftouter")
    viVals = joined.select("vi_address", "vo_value")
    addressesSentValue = viVals.groupBy(viVals.vi_address).agg({"vo_value": "sum"}).withColumnRenamed("SUM(vo_value)", "sent")

    # Recv addresses
    print("Calculating received values")
    voVals = dfvo.select("vo_pubkey", "vo_value")
    addressesRecValue = voVals.groupBy(voVals.vo_pubkey).agg({"vo_value": "sum"}).withColumnRenamed("SUM(vo_value)", "recv")
    unioned = addressesRecValue.join(addressesSentValue,addressesRecValue.vo_pubkey == addressesSentValue.vi_address,"outer").cache()

    # Amount remaining
    print("Calculating remaining amounts of addresses")
    unioned = unioned.na.fill({"sent": 0.0, "recv": 0.0})
    unioned = unioned.withColumn('current_value', func.col('recv')-func.col('sent'))

    # Number of tx sent
    print("Calculating number of txs sent")
    txs_sent = dfvi.select("vi_address", "vi_tx_hash").distinct().groupBy("vi_address").count().withColumnRenamed("count", "txs_sent")
    unioned = unioned.join(txs_sent, "vi_address", "outer")

    # Number of tx recv
    print("Calculating number of txs received")
    tx_recv = dfvo.select("vo_pubkey", "vo_tx_hash").distinct().groupBy("vo_pubkey").count().withColumnRenamed("count","txs_recv")
    unioned = unioned.join(tx_recv, "vo_pubkey", "outer")

    # Number of vins
    print("Calculating the number of inputs")
    vins_count = dfvi.select("vi_address", "vi_tx_hash").groupBy("vi_address").count().withColumnRenamed("count", "vins_count")
    unioned = unioned.join(vins_count, "vi_address", "outer")

    # Number of vouts
    print("Calculating the number of outputs")
    vouts_count = dfvo.select("vo_pubkey", "vo_tx_hash").groupBy("vo_pubkey").count().withColumnRenamed("count","vouts_count")
    unioned = unioned.join(vouts_count, "vo_pubkey", "outer")

    # Number of transactions total
    print("Calculating the number of transactions involved in total")
    unioned = unioned.withColumn('no_txs_total', func.col('txs_sent') + func.col('txs_recv'))

    # Number of blocks mined
    print("Calculating the number of blocks mined")
    coins_mined = dfc.select("c_tx_hash")
    joined = coins_mined.join(dfvo, (coins_mined.c_tx_hash == dfvo.vo_tx_hash), "leftouter")
    coins_mined_count = joined.groupBy("vo_pubkey").count().withColumnRenamed("count","coingens_recv")
    unioned = unioned.join(coins_mined_count, "vo_pubkey", "outer")

    # Calculate pool amount per address
    print("Estimating amount of coins sent/recv from pool per address")
    dftx = dft.where(dft.t_type == "deshielded")
    dftvox = dfvo.join(dftx, dfvo.vo_tx_hash == dftx.t_tx_hash)
    dftvox = dftvox.selectExpr("vo_pubkey as address", "vo_value as value")
    outputs = dftvox.groupBy(['address']).sum('value').withColumnRenamed("sum(value)", "value").collect()
    dfvjx = dfvj.where(func.size(func.col("vj_source_address")) == 1)
    dfvjx = dfvjx.selectExpr("vj_vpub_old as value", "vj_source_address as address")
    inputs = dfvjx.groupBy(['address']).sum('value').withColumnRenamed("sum(value)", "value").collect()
    poolValuesPerAddress = {}
    for row in outputs:
        poolValuesPerAddress[row.address[0]] = {"input_pool": 0, "output_pool": row.value}
    addresses = set(poolValuesPerAddress.keys())
    for row in inputs:
        if row.address[0] in addresses:
            poolValuesPerAddress[row.address[0]]["input_pool"] = row.value
        else:
            poolValuesPerAddress[row.address[0]] = {"input_pool": row.value, "output_pool": 0}
    poolValuesPerAddressDF = sc.parallelize([
        (k, v['input_pool'], v['output_pool'])
        for k, v in poolValuesPerAddress.iteritems()
    ]).coalesce(4).toDF(['vo_pubkey', 'pool_sent', 'pool_recv'])


    udf_flat = udf(flatudf, StringType())
    unioned = unioned.withColumn('vo_pubkey_flat', udf_flat(unioned.vo_pubkey))\
        .drop('vo_pubkey') \
        .withColumnRenamed('vo_pubkey_flat', 'vo_pubkey')
    unioned = unioned.withColumn('vi_address_flat', udf_flat(unioned.vi_address))\
        .drop('vi_address')\
        .withColumnRenamed('vi_address_flat', 'vi_address')
    # Total sent  and recv to the pool
    print("Joining pool amounts to addresses")
    unioned = unioned.join(poolValuesPerAddressDF, "vo_pubkey", "outer")\

    # fill empty
    print("Filling in empty values")
    unioned = unioned.fillna({
        "pool_recv": 0.0,
        "pool_sent": 0.0,
        "coingens_recv": 0,
        "vouts_count": 0,
        "vins_count": 0,
        "txs_recv": 0,
        "txs_sent":0,
        "recv": 0.0,
        "sent":0.0,
        "no_txs_total":0,
    })

    unioned = unioned.na.fill({
        "pool_recv": 0.0,
        "pool_sent": 0.0,
        "coingens_recv": 0,
        "vouts_count": 0,
        "vins_count": 0,
        "txs_recv": 0,
        "txs_sent": 0,
        "recv": 0.0,
        "sent": 0.0,
        "no_txs_total": 0,
    })

    # Pickle
    unioned = unioned.cache()
    print("Collecting and dumping to pickle address_values.pkl")
    addressValues = unioned.collect()
    pickle.dump(addressValues, open(path+"address_values.pkl", "w"))

    # Save as CSV
    print("Saving to CSV addresses_values.csv")
    unioned.toPandas().to_csv(path+"addresses_values.csv", index=False, sep=";")

    # rich list top 10 most recv
    print("Generating rich list")
    print("Top 10 addresses that received coins")
    top10recv = unioned.sort(unioned.recv.desc()).take(10)
    results = ("Address;Total Coins received\n")
    for row in top10recv:
        addr = row["vo_pubkey"]
        if addr == "" or addr == None:
            addr = row["vi_address"]
        results += str(addr)+ ";"+str(row["recv"])+"\n"
    filename = "rich_list_top_10_recv.csv"
    print("Writing results to file %s%s" % (conf.pathresearch, filename))
    resultsfile = open(conf.pathresearch+filename, "w")
    resultsfile.writelines(results)
    print(results)

    # rich list top 10 most sent
    # write results to disk
    print("Top 10 addresses that sent coins")
    top10sent = unioned.sort(unioned.sent.desc()).take(10)
    results = "Address;Total Coins sent\n"
    for row in top10sent:
        addr = row["vo_pubkey"]
        if addr == "" or addr == None:
            addr = row["vi_address"]
        results += str(addr)+ ";"+str(row["sent"])+"\n"
    filename = "rich_list_top_10_sent.csv"
    print("Writing results to file %s%s" % (conf.pathresearch, filename))
    resultsfile = open(conf.pathresearch+filename, "w")
    resultsfile.writelines(results)
    resultsfile.close()
    print(results)

    # rich list, current value richest
    print("Top 10 addresses with current balance")
    top10current = unioned.sort(unioned.current_value.desc()).take(10)
    results = "Address;Current Amount\n"
    for row in top10current:
        addr = row["vo_pubkey"]
        if addr == "" or addr == None:
            addr = row["vi_address"]
        results += str(addr)+ ";"+str(row["current_value"])+"\n"
    filename = "rich_list_top_10_value.csv"
    print("Writing results to file %s%s" % (conf.pathresearch, filename))
    resultsfile = open(conf.pathresearch+filename, "w")
    resultsfile.writelines(results)
    resultsfile.close()
    print(results)


def getSizesofCommitmentsAndNullifiers(sc, sql_sc, file_vj_csv):
    vj = sc.textFile(file_vj_csv)
    vj = vj.map(lambda x: x.split(';'))
    vj = vj.filter(lambda x: x[0] != u'db_id')
    vj = vj.map(lambda x: [
        x[0],
        x[1],
        Decimal(x[2]),
        Decimal(x[3]),
        x[4],
        cleanAndSplit(x[5]),
        cleanAndSplit(x[6]),
        cleanAndSplit(x[7]),
        x[8],
        cleanAndSplit(x[9]),
        cleanAndSplit(x[10]),
        cleanAndSplit(x[11])
    ])
    vjSchema = sqlTypes.StructType(
        [
            sqlTypes.StructField("vj_db_id", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_tx_hash", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_vpub_old", sqlTypes.DecimalType(32,8), True),
            sqlTypes.StructField("vj_vpub_new", sqlTypes.DecimalType(32,8), True),
            sqlTypes.StructField("vj_anchor", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_nullifiers", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            sqlTypes.StructField("vj_commitments", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            sqlTypes.StructField("vj_onetimePubKey", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_randomSeed", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_macs", sqlTypes.StringType(), True),
            sqlTypes.StructField("vj_source_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
            sqlTypes.StructField("vj_dest_address", sqlTypes.ArrayType(sqlTypes.StringType()), True),
        ])

    dfvj = sql_sc.createDataFrame(vj, vjSchema)
    vj.unpersist()
    dfvj = dfvj.cache()
    dfvj.count()

    dfvj.groupby(func.size(dfvj.vj_nullifiers)).agg({"vj_nullifiers": "count"}).show()
    dfvj.groupby(func.size(dfvj.vj_commitments)).agg({"vj_commitments": "count"}).show()


def countByTtype(dft):
    dft.select(dft.t_type).groupBy("t_type").count().show()


def TtoTStats(dft, dfb, dfc, dfvi):
    # Total number of T-T public
    # Percentage this is out of them all
    # amount of ZEC used in every TX
    # amount of ZEC used in only T-T
    # do them all as well fam
    print "Total number of tx " + str(dft.count())
    public = dft.where(dft.t_type=="public")
    print "Number of T-T (public): " + str(public.count())
    print "Percentage of T-T " + str((public.count()/dft.count())*100)
    # ignore coingens
    dfthashes = dft.where(dft.type!="minerReward").select(dft.t_tx_hash)
    x = dfthashes.join
