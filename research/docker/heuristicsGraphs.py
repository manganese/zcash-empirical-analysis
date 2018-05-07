from __future__ import print_function
import os
from founders import trivialFounders
from pools import pools
from pyspark.sql.functions import array_contains, col, size
from collections import Counter
import matplotlib as mpl

mpl.use('Agg')
from plotGraphs import *
import config as conf
import helpfulFunctions as hf
import sys

sc, sql_sc = hf.initiateSpark()


def dumpToCsv(filename, data):
    with open(conf.pathresearch + filename + ".csv", "w") as fw:
        fw.write("addresses\n")
        for element in data:
            fw.write("{}\n".format(element))

def createDataframes():
    dirx = conf.pathresearch
    dfvout = sql_sc.read.parquet(dirx + "vout.parquet")
    dfc = sql_sc.read.parquet(dirx + "coingen.parquet")
    dft = sql_sc.read.parquet(dirx + "transaction.parquet")
    dfb = sql_sc.read.parquet(dirx + "block.parquet")
    dfvj = sql_sc.read.parquet(dirx + "vjoinsplit.parquet")
    dfvin = sql_sc.read.parquet(dirx + "vin.parquet")

    return dft, dfb, dfvj, dfc, dfvout, dfvin

def collectTrivialMiners(dfc, dfvout):
    tmp = dfvout.join(dfc, dfc.c_tx_hash == dfvout.vo_tx_hash)
    addresses = tmp.selectExpr("vo_pubkey as a").collect()
    addresses = [row.a[0] for row in addresses]
    addresses = set(addresses)
    miners = [a for a in addresses if a not in trivialFounders]
    miners = set(miners)

    return miners


def typesPerBlock(dft, dfb):
    tmp = dft.join(dfb, dft.t_blockhash == dfb.b_hash)
    tmp = tmp.selectExpr("b_height as height", "t_type as type").collect()
    data = [[0, 0, 0, 0, 0, 0] for _ in range(max([row.height for row in tmp]) + 1)]

    for row in tmp:
        if row.type == "private":
            data[row.height][0] += 1
        elif row.type == "public":
            data[row.height][1] += 1
        elif row.type == "shielded":
            data[row.height][2] += 1
        elif row.type == "deshielded":
            data[row.height][3] += 1
        elif row.type == "mixed":
            data[row.height][4] += 1
        else:
            data[row.height][5] += 1

    return data


def totalValueOverTime(dft, dfb, dfvj):
    tmp = dfvj.join(dft, dfvj.vj_tx_hash == dft.t_tx_hash)
    tmp = tmp.join(dfb, tmp.t_blockhash == dfb.b_hash)
    tmp = tmp.selectExpr("vj_vpub_old as o", "vj_vpub_new as n", "b_height as h").collect()

    data = [0 for _ in range(max([row.h for row in tmp]) + 1)]

    for row in tmp:
        data[row.h] += row.o
        data[row.h] -= row.n

    return data


def poolDeposits(dft, dfb, dfvj, dfvin):
    tmp = dfvj.where(dfvj.vj_vpub_old > 0)
    tmp = tmp.join(dft, tmp.vj_tx_hash == dft.t_tx_hash)
    tmp = tmp.join(dfb, tmp.t_blockhash == dfb.b_hash)
    data = tmp.selectExpr("vj_source_address as a", "vj_vpub_old as v", "b_height as h").collect()

    return data


def poolWithdrawals(dft, dfb, dfvj, dfvout):
    tmp = dft.where(dft.t_type == "deshielded")
    tmp = tmp.join(dfvout, tmp.t_tx_hash == dfvout.vo_tx_hash)
    tmp = tmp.join(dfb, tmp.t_blockhash == dfb.b_hash)
    data = tmp.selectExpr("vo_pubkey as a", "vo_value as v", "b_height as h").collect()

    return data


def depositsWithdrawalsEachBlock(dft, dfb, dfvj, blockHeight):
    tmp = dfvj.join(dft, dfvj.vj_tx_hash == dft.t_tx_hash)
    tmp = tmp.join(dfb, tmp.t_blockhash == dfb.b_hash)
    data = tmp.selectExpr("vj_vpub_old as o", "vj_vpub_new as n", "b_height as h").groupBy(['h']).sum('n',
                                                                                                      'o').withColumnRenamed(
        "sum(n)", "n").withColumnRenamed("sum(o)", "o")
    
    data=data.collect()
    tmp = [[0, 0] for _ in range(blockHeight+1)]
    for row in data:
        tmp[row.h][0] = row.n
        tmp[row.h][1] = row.o
    data = tmp

    return data


def defineHeuristic3Founders(dfvj):
    tmp = dfvj.where(dfvj.vj_vpub_new == 250.0001)
    tmp = tmp.selectExpr("vj_dest_address as a").collect()
    nonTrivialFounders = []
    for row in tmp:
        nonTrivialFounders += row.a

    nonTrivialFounders = set(nonTrivialFounders)

    return nonTrivialFounders


def defineHeuristic4Miners(dfvj):
    p = list(pools)

    test = dfvj.where(size(col("vj_dest_address")) > 100).where(array_contains(col("vj_dest_address"), p[0]))
    for pool in p:
        tmp = dfvj.where(size(col("vj_dest_address")) > 100).where(array_contains(col("vj_dest_address"), pool))
        test = test.unionAll(tmp)

    tmp = test.selectExpr("vj_dest_address as a").collect()

    nonTrivialMiners = []
    for row in tmp:
        nonTrivialMiners += row.a
    nonTrivialMiners = set(nonTrivialMiners)

    return nonTrivialMiners


def heuristic5(dfvj, dirx):

    dfvj = dfvj.join(dft, dfvj.vj_tx_hash == dft.t_tx_hash)
    dfvj = dfvj.join(dfb, dfb.b_hash == dfvj.t_blockhash)
    dfvj = dfvj.selectExpr("vj_tx_hash as hash", "vj_vpub_old as vOld", "vj_vpub_new as vNew", "b_height as height")

    inputs = dfvj.where(dfvj.vOld > 0).collect()
    outputs = dfvj.where(dfvj.vNew > 0).collect()

    c1 = Counter([row.vOld for row in inputs])
    c2 = Counter([row.vNew for row in outputs])

    inputs = [i for i in inputs if c1[i.vOld] == 1]
    outputs = [o for o in outputs if c2[o.vNew] == 1]

    uniques = set([row.vNew for row in outputs])

    inputs = [i for i in inputs if i.vOld in uniques]

    d = {}

    for i in inputs:
        d[i.vOld] = [i]

    s = set(d.keys())

    for o in outputs:
        if o.vNew in s:
            d[o.vNew].append(o)

    dNew = {}

    for k, v in d.iteritems():
        if v[1].height > v[0].height:
            dNew[k] = v

    report = [(v[0].hash, v[1].hash) for v in dNew.values()]

    with open(dirx+"heuristic5.txt", "w") as fw:
        for element in report:
            fw.write("{} --> {}\n".format(element[0], element[1]))

    fw.close()

    return


def createGraphFolder(dirx):
    if not os.path.exists(dirx + 'Graphs'):
        os.mkdir(dirx + 'Graphs')


if __name__ == "__main__":
    # Create the dataframes which will be used for our analysis.
    print ("Creating Dataframes...")
    dft, dfb, dfvj, dfc, dfvout, dfvin = createDataframes()
    print ("Done!")

    blockHeight=conf.blockheight
    check=sys.argv[1:]
    whitelist=["2","4","5","6","8a","8b","8c","9","h3","h4","h5"]

    flag=False
    for element in check:
        if element not in whitelist:
                print("{} is an invalid argument.".format(element))
                flag=True
        elif check.count(element)>1:
                flag=True
                print("You added argument {}, {} times.".format(element, check.count(element)))
                check=[c for c in check if c!=element]
    if flag:
        sys.exit()


    # Create graph folder
    createGraphFolder(conf.pathresearch)

    # Collect the trivial Miners addresses who are the recipients of coingen transactions
    print ("Collecting Miners Addresses...")
    trivialMiners = collectTrivialMiners(dfc, dfvout)
    print ("Done!")

    # Dump the addresses of the trivial miners in miners_addresses.csv
    dumpToCsv("miners_addresses", trivialMiners)

    if "2" in check:
        # Create data for our first basic graph
        # Transaction types over time, fig.2
        print ("Collecting data for Transaction types per Block...")
        dataForTransactionTypesPerBlock = typesPerBlock(dft, dfb)
        print ("Done!")

        # Plotting Figure2
        print ("Plotting TransactionTypes.pdf...")
        plotTransactionTypes(dataForTransactionTypesPerBlock)
        print ("Done!")

    if "4" in check:
        # Create data for the value of the pool over time, fig.4
        print ("Collecting data for Total Value of the Pool over time...")
        dataForTotalValueOverTime = totalValueOverTime(dft, dfb, dfvj)
        print ("Done!")

        # Plotting Figure4
        print ("Plotting TotalValueOverTime.pdf...")
        plotTotalValueOverTime(dataForTotalValueOverTime)
        print ("Done!")

    if "5" in check:
        # Create data for the symmetric Withdrawal-Deposits in each block, fig.5
        print ("Collecting data for Withdrawals-Deposits in each block...")
        dataForDepositsWithdrawals = depositsWithdrawalsEachBlock(dft, dfb, dfvj, blockHeight)
        print ("Done!")

        # Plotting Figure5
        print ("Plotting Deposits-Withdrawals.pdf...")
        plotDepositsWithdrawals(dataForDepositsWithdrawals)
        print ("Done!")

    if "6" in check:
        # Create data for the Pool Deposits
        print ("Collecting data for Pool Deposits...")
        dataForPoolDeposits = poolDeposits(dft, dfb, dfvj, dfvin)
        print ("Done!")

        # Plotting Figure6
        print ("Plotting DepositsPerIdentity.pdf...")
        interactionType = "DepositsPerIdentity"
        plotInteractionsPerIdentity(dataForPoolDeposits, trivialMiners, trivialFounders, interactionType, blockHeight)
        print ("Done!")

    if "8a" in check or "8b" in check or "8c" in check:

        # Create data for the Pool Withdrawals
        print ("Collecting data for Pool Withdrawals...")
        dataForPoolWithdrawals = poolWithdrawals(dft, dfb, dfvj, dfvout)
        print ("Done!")

    
    if "8b" in check or "8c" in check or "h3" in check:
        # Collect the Founders addresses, using Heuristic 3 and dump them
        print ("Defining Founders of Heuristic 3...")
        nonTrivialFounders = defineHeuristic3Founders(dfvj)
        print ("Done!")

        #founders_heuristic_addresses.csv contains the addresses of the identified Founders Addresses through Heuristic3
        dumpToCsv("founders_heuristic_addresses", nonTrivialFounders)

    if "8c" in check or "h4" in check:
        # Collect the Miners addresses, using Heuristic 4 and dump them
        print ("Defining Miners of Heuristic 4...")
        nonTrivialMiners = defineHeuristic4Miners(dfvj)
        print ("Done!")

        #miners_heuristic_addresses.csv contains the addresses of the identified Miners Addresses through Heuristic4
        dumpToCsv("miners_heuristic_addresses", nonTrivialMiners)

    if "8a" in check:
        # Plotting Figure8a
        print ("Plotting WithdrawalsPerIdentityNoHeuristic.pdf...")
        interactionType = "WithdrawalsPerIdentityNoHeuristic"
        plotInteractionsPerIdentity(dataForPoolWithdrawals, trivialMiners, trivialFounders, interactionType, blockHeight)
        print ("Done!")

    if "8b" in check:
        # Plotting Figure8b
        print ("Plotting WithdrawalsPerIdentityHeuristicF.pdf...")
        interactionType = "WithdrawalsPerIdentityHeuristicF"
        plotInteractionsPerIdentity(dataForPoolWithdrawals, trivialMiners, nonTrivialFounders, interactionType, blockHeight)
        print ("Done!")

    if "8c" in check:
        # Plotting Figure8c
        print ("Plotting WithdrawalsPerIdentityHeuristicFM.pdf...")
        interactionType = "WithdrawalsPerIdentityHeuristicFM"
        plotInteractionsPerIdentity(dataForPoolWithdrawals, nonTrivialMiners, nonTrivialFounders, interactionType,
                                    blockHeight)
        print ("Done!")

    if "9" in check:
        print ("Collecting data for FoundersCorrelation.pdf...")
        inputs, outputs = correlateFounders(dfvj, dft, dfb, blockHeight)
        print ("Done!")

        print ("Plotting FoundersCorrelation.pdf...")
        plotFoundersCorrelation(inputs, outputs, blockHeight)
        print ("Done!")

    if "h5" in check:
        print ("Creating report for Heuristic 5...")
        heuristic5(dfvj, conf.pathresearch)
        print ("Done!")

