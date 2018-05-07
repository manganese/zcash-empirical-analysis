'''
This is the file for heuristic 1.
It computes address statistics, clusters the addresses and creates cluster statistics. 

    1. It loads the transactions and source addresses from the parquet files. 
    2. It then creates an empty undirected graph
    3. For each transaction it takes out all the addresses. Then it makes an undirected path 
       from the first address to every other address. Effectively this means it says that, 
       these addresses are all linked because they are included in the same input
    4. It repeats this for every single transaction
    5. This leads to a large, undirected graph, with components that are linked
    6. From this, we extract the connected components. 
       Connected components are all the pairs of nodes that are linked together. 
       These subgraphs make up the clusters.
    7. It then does dumps the entire cluster set to a python pkl files
    8. Then it computes some basic stats over the clusters: 
       total unique addresses, total number of txs, largest cluster, 
       sizes of the top 5, 10, 25, 50, 100 clusters 
    9. Optionally you can run the function to peform a sanity check, this takes the longest amount of time. 
       The sanity check compares every single cluster to each other cluster and does a set intersection.
       Why a set intersection? To identify if a) an address is found once, and only once,
       in all clusters. If an address was found in another cluster, then there has been a mistake. If this prints that 0 intersections were found, it means 
       every cluster is unique 
'''

import sys
import networkx as nx
import time
import pickle
import itertools
import helpfulFunctions as hf
import pyspark.sql.functions as func
import operator
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import config as conf
from decimal import *
getcontext().prec = 8


def cleanlist(x):
    return list(set([item for sublist in x for item in sublist]))


def generateSmallTXlist(dft, dfvi, dfvo, dfvj):
    print("Mapping vout")
    vout = dfvo.select("vo_tx_hash", "vo_pubkey")
    voutgbk = vout.groupBy("vo_tx_hash").agg(func.collect_list("vo_pubkey")).withColumnRenamed("collect_list(vo_pubkey)", "vo_pubkey")
    flat_udf = udf(cleanlist, ArrayType(StringType()))
    dfvoutgbk = voutgbk.select("vo_tx_hash", flat_udf("vo_pubkey")).withColumnRenamed("cleanlist(vo_pubkey)", "vo_pubkey")

    print("Creating dataframe")
    dft = dft.select("t_tx_hash", "t_blockhash")
    dfvoutgbk = dfvoutgbk.withColumnRenamed("vo_pubkey", "dest_addresses")
    dfTVo = dft.join(dfvoutgbk, dft["t_tx_hash"] == dfvoutgbk["vo_tx_hash"], "left_outer")
    dfTVo = dfTVo.selectExpr("t_tx_hash as tx_hash",
                             "t_blockhash as blockhash",
                             "dest_addresses as dest_addresses",
                             )
    # that means tx dest address is done
    # now crate
    print("Creating vins")
    dfvi = dfvi.select("vi_tx_hash", "vi_address")
    dfvi = dfvi.groupBy("vi_tx_hash").agg(func.collect_list("vi_address")).withColumnRenamed("collect_list(vi_address)", "vi_address")
    dfvigbk = dfvi.select("vi_tx_hash", flat_udf("vi_address")).withColumnRenamed("cleanlist(vi_address)", "src_addresses")

    print("Joining vins and vouts")
    dfTVinVo = dfTVo.join(dfvigbk, dfTVo["tx_hash"] == dfvigbk["vi_tx_hash"], "left_outer")
    dfTVinVo = dfTVinVo.selectExpr("tx_hash as tx_hash",
                                   "blockhash as blockhash",
                                   "dest_addresses as dest_addresses",
                                   "src_addresses as src_addresses",
                                   )
    return dfTVinVo


def generateClusters(txs):
    print "generating graph"
    print time.strftime("%d-%m-%Y %H:%M:%S")
    txs = txs.where(func.size('src_addresses')>1).collect()
    graph = nx.MultiGraph()
    num = 0
    total = len(txs)
    distincttransactions = {}
    for tx in txs:
        num += 1
        if num % 10000 == 0:
            print str(num) + "/" + str(total)
        tx_hash = tx['tx_hash']
        nodes = tx['src_addresses']
        # add the links
        nodeBefore = nodes[0]
        firstRun = True
        for node in nodes:
            # if node exists leave it
            # else add the node
            if not graph.has_node(node):
                graph.add_node(node)
            if node not in distincttransactions:
                distincttransactions[node] = set()
            distincttransactions[node].add(tx_hash)
            # create the a link between the first node and next node, graph is undirected
            graph.add_path([nodeBefore, node], tx_hash=tx_hash)
            if firstRun:
                firstRun = False
            if not firstRun:
                nodeBefore = node
    # we only join the first node to every other, why? because its an multigraph graph, it makes the computation
    # faster plus, we dont really care about how many nodes linked to others, rather if there was a link at all
    # we then look for the connected components, cause these are the clusters
    print "Graph complete"
    # Connected subs
    print time.strftime("%d-%m-%Y %H:%M:%S")
    print "generating sorted clusters"
    clusters = [c for c in sorted(nx.connected_components(graph), key=len, reverse=True)]
    print "done "+ time.strftime("%d-%m-%Y %H:%M:%S")
    return clusters, graph, distincttransactions


def clusterStats(txs, clusters, graph, addressStats, distinctnodeandtxs, writeToFile, path):
    # get address stats
    # this takes a while cause pkl is big man, im talking gigas
    md = "# Heuristic 1 info\n\n"
    print "Finding stats"
    print time.strftime("%d-%m-%Y %H:%M:%S")
    word = "- Number of communities/clusters : " + str(nx.number_connected_components(graph))
    print word
    md += word +"\n"
    print time.strftime("%d-%m-%Y %H:%M:%S")
    print "       - From the clusters var : " + str(len(clusters))
    # largest community
    print time.strftime("%d-%m-%Y %H:%M:%S")
    print "largest start"
    largest_cluster = clusters[0]
    word = "- Largest cluster : " + str(len(largest_cluster))
    print word
    md += word + "\n"
    # top 100 cluster sizes
    allSizes = [len(c) for c in clusters]
    word = "- Top 5 clusters contain   : " + str(sum(allSizes[:5]))   + " addresses\n"
    word +="- Top 10 clusters contain  : " + str(sum(allSizes[:10]))  + " addresses\n"
    word +="- Top 25 clusters contain  : " + str(sum(allSizes[:25]))  + " addresses\n"
    word +="- Top 50 clusters contain  : " + str(sum(allSizes[:50]))  + " addresses\n"
    word +="- Top 100 clusters contain : " + str(sum(allSizes[:100])) + " addresses\n"
    print word
    md += word + "\n"
    print time.strftime("%d-%m-%Y %H:%M:%S")
    # now verify the clusters
    # # total number of addresses - cba to write this
    noAddresses = graph.number_of_nodes()
    word = "- Total number of addresses that have sent transactions: " + str(noAddresses) + "\n"
    word += "- Total number of txs processed :" + str(txs.count()) +"\n"
    print word
    md += word + "\n"
    print "Now trying to tag all the clusters with the known addresses!"
    word = "Cluster's analysis"
    print word
    md += word + "\n"
    num = 0
    clusterWithStats = {}
    addressValues = hf.loadAddressesFromCsv()
    poolAddresses = set(addressValues['poolAddresses'].keys())
    pools = addressValues['poolAddresses'].keys()
    foundersAddresses = addressValues['foundersAddress']
    minersAddresses = addressValues['minersAddresses']
    minersHeuristicAddresses = addressValues['minersHeuristicAddresses']
    foundersHeuristicAddresses = addressValues['foundersHeuristicAddresses']
    addressTags = addressValues['addressTags']
    taggedAddresses = set(addressValues['addressTags'].keys())
    totalMinersHeuristic = float(len(minersHeuristicAddresses))
    totalFoundersandHeuristic = float(len(foundersHeuristicAddresses.union(foundersAddresses)))

    for cluster in clusters:
        # print "\n************"
        clusterset = set(cluster)
        poolsFound = list(clusterset.intersection(poolAddresses))
        minersFound = list(clusterset.intersection(minersAddresses))
        foundersFound = list(clusterset.intersection(foundersAddresses))
        heuristicMinersFound = list(clusterset.intersection(minersHeuristicAddresses))
        heuristicFoundersFound = list(clusterset.intersection(foundersHeuristicAddresses))
        taggedFound = list(clusterset.intersection(taggedAddresses))

        total_coingens_recv = 0.0
        total_current_value = 0.0
        total_no_txs_total =0
        distinct_txs = []
        total_pool_recv = Decimal('0.0')
        total_pool_sent = Decimal('0.0')
        total_recv = Decimal('0.0')
        total_sent = Decimal('0.0')
        total_txs_recv = 0
        total_txs_sent = 0
        total_vins_count = 0
        total_vouts_count = 0
        for addr in list(clusterset):
            txhashes = distinctnodeandtxs[str(addr)]
            if str(addr) in addressStats:
                total_coingens_recv += addressStats[addr]['coingens_recv']
                total_current_value += float(addressStats[addr]['current_value'])
                total_no_txs_total += addressStats[addr]['no_txs_total']
                distinct_txs.extend(list(txhashes))
                total_pool_recv += addressStats[addr]['pool_recv']
                total_pool_sent += addressStats[addr]['pool_sent']
                total_recv += Decimal(addressStats[addr]['recv'])
                total_sent += addressStats[addr]['sent']
                total_txs_recv += addressStats[addr]['txs_recv']
                total_txs_sent += addressStats[addr]['txs_sent']
                total_vins_count += addressStats[addr]['vins_count']
                total_vouts_count += addressStats[addr]['vouts_count']
            else:
                print "cant find address"
                print addr

        distinct_txs = list(set(distinct_txs))
        total_distinct_txs = len(distinct_txs)
        namedTags = []
        # if any pools are found, then yes, this cluster is interesting so write about it
        poolList = []
        if len(poolsFound) or len(minersFound) or len(foundersFound) or len(heuristicMinersFound) or len(heuristicFoundersFound) or len(taggedFound):
            word =  "- cluster "+str(num)+" was interesting\n"
            word += "  - " + str(len(cluster)) + " addresses\n"
            word += "  - " + str(len(set(cluster)) - len(cluster)) + " duplicates\n"
            if len(poolsFound):
                word += "  - Mining Pools found : " + str(set(poolsFound)) + "\n"
                poolList = []
                for addrx in poolsFound:
                    poolList.append(pools[addrx])
                word += "  - Mining pools Names : " + str(set(poolList)) + "\n"
            if len(minersFound):
                word += "  - Miners found : " + str(set(minersFound)) + "\n"
            if len(foundersFound):
                word += "  - Founders found : " + str(set(foundersFound)) + "\n"
            if len(heuristicMinersFound):
                word += "  - Mining Heuristic found : " + str(len(set(heuristicMinersFound))) + "\n"
                word += "  - Percentage of miners heuristic : " + str((float(len(set(heuristicMinersFound)))/totalMinersHeuristic)*100)
            if len(heuristicFoundersFound):
                word += "  - Founders Heuristic found : " + str(len(set(heuristicFoundersFound))) + "\n"
                word += "  - Percentage of founders+heur : " + str(((len(set(heuristicFoundersFound))+len(foundersFound))/totalFoundersandHeuristic)*100)
            if len(taggedFound):
                namedTags = []
                for taggedaddr in taggedFound:
                    namedTags.append(addressTags[taggedaddr])
                namedTags = list(set(namedTags))
                word += "  - Tags found : " + str(namedTags) + "\n"
            word += "  - total_coingens_recv : " + str(total_coingens_recv)+"\n"
            word += "  - total_current_value : " + str(total_current_value)+"\n"
            word += "  - total_no_txs_total : " + str(total_no_txs_total)+"\n"
            word += "  - distinct_total_no_txs : " + str(total_distinct_txs)+"\n"
            word += "  - total_pool_recv : " + str(total_pool_recv)+"\n"
            word += "  - total_pool_sent : " + str(total_pool_sent)+"\n"
            word += "  - total_recv : " + str(total_recv)+"\n"
            word += "  - total_sent : " + str(total_sent)+"\n"
            word += "  - total_txs_recv : " + str(total_txs_recv)+"\n"
            word += "  - total_txs_sent : " + str(total_txs_sent)+"\n"
            word += "  - total_vins_count : " + str(total_vins_count)+"\n"
            word += "  - total_vouts_count : " + str(total_vouts_count)+"\n"
            print word
            md += word
            md += "\n"

        clusterWithStats[num] = {
            "number" : num,
            "cluster" : cluster,
            "pools" : list(set(poolList)),
            "miners": list(set(minersFound)),
            "founders": len(foundersFound),
            "MinersHeuristic":list(set(heuristicMinersFound)),
            "FoundersHeuristic":list(set(heuristicFoundersFound)),
            "size" : len(cluster),
            "total_coingens_recv": total_coingens_recv,
            "total_current_value": total_current_value,
            "total_no_txs_total": total_no_txs_total,
            "total_pool_recv": total_pool_recv,
            "total_pool_sent": total_pool_sent,
            "total_recv": total_recv,
            "total_sent": total_sent,
            "total_txs_recv": total_txs_recv,
            "total_txs_sent": total_txs_sent,
            "total_vins_count": total_vins_count,
            "total_vouts_count":total_vouts_count,
            "distinct_txs": distinct_txs,
            "total_distinct_txs":total_distinct_txs,
            "tags": namedTags,
        }
        num += 1
    if writeToFile:
        createMarkdownFileCauseImTooLazyToWriteItMyself(md, path)
    print time.strftime("%d-%m-%Y %H:%M:%S")
    return clusterWithStats


def writeToDisk(clusters, graph, clustersWithStats, path):
    print("Dumping all clusters to python pickled file")
    with open(path+'clusters_stats.pkl', 'w') as f:
        pickle.dump(clustersWithStats, f)
    print("Done")
    print("Generating CSV file")
    with open(path+'cluster_stats.csv', 'w') as f:
        # print header
        w = "id;"
        for key in clustersWithStats[0].keys():
            w = w + str(key) + ";"
        f.write(w[:-1]+"\n")
        for key in clustersWithStats.iterkeys():
            w = str(key)
            line = w + ";" + str(clustersWithStats[key]['total_txs_recv']) + ";" + str(
                clustersWithStats[key]['total_coingens_recv']) + ";" + str(
                clustersWithStats[key]['total_vouts_count']) + ";" + str(
                clustersWithStats[key]['founders']) + ";" + str(clustersWithStats[key]['total_vins_count']) + ";" + str(
                len(clustersWithStats[key]['FoundersHeuristic'])) + ";" + str(
                clustersWithStats[key]['total_txs_sent']) + ";" + str(
                clustersWithStats[key]['total_no_txs_total']) + ";" + str(clustersWithStats[key]['number']) + ";" + str(
                len(clustersWithStats[key]['miners'])) + ";" + str(len(clustersWithStats[key]['cluster'])) + ";" + str(
                clustersWithStats[key]['total_pool_sent']) + ";" + str(
                clustersWithStats[key]['total_current_value']) + ";" + str(
                clustersWithStats[key]['total_recv']) + ";" + str(clustersWithStats[key]['total_sent']) + ";" + str(
                len(clustersWithStats[key]['pools'])) + ";" + str(clustersWithStats[key]['total_pool_recv']) + ";" + str(
                clustersWithStats[key]['size']) + ";" + str(len(clustersWithStats[key]['MinersHeuristic'])) + "\n"
            f.write(line)
    f.close()
    print("Dumping entire graph to pickle")
    with open(path+'clusters_graph.pkl', 'w') as f:
        pickle.dump(graph, f)
    print("Done")


def quickVerify(clusters):
    # set intersection between every single cluster (if unique result should equal zero
    # if it equals zero it means that every address is in one, and only one, cluster
    # clusters have unique addresses
    intersectionsFound = 0
    print time.strftime("%d-%m-%Y %H:%M:%S")
    for clusterA, clusterB in itertools.combinations(clusters, 2):
        if len(clusterA.intersection(clusterB)) != 0:
            intersectionsFound += 1
            print "ffff"
    print "After mixing every cluster together I found " + str(intersectionsFound)
    print time.strftime("%d-%m-%Y %H:%M:%S")


def createMarkdownFileCauseImTooLazyToWriteItMyself(md, path):
    with open(path+"README-heuristic1.md", "w") as f:
        f.write(md)


def findCluster0Txs(graph):
    # print it into a network graph, color the ones we know, label the rest
    # this one takes a long time 
    print time.strftime("%d-%m-%Y %H:%M:%S")
    bigone = max(nx.connected_component_subgraphs(graph), key=len)
    print time.strftime("%d-%m-%Y %H:%M:%S")
    return bigone


def topAddrcluster(cluster, received, sent):
    yaz = set(cluster).intersection(set(received.keys()))
    addrx = {}
    for addr in list(yaz):
        addrx[addr] = received[addr]
    sorted_x = sorted(addrx.items(), key=operator.itemgetter(1), reverse=True)

    yazSent = set(cluster).intersection(set(sent.keys()))
    addrxSent = {}
    for addr in list(yazSent):
        addrxSent[addr] = sent[addr]
    sorted_xsent = sorted(addrxSent.items(), key=operator.itemgetter(1), reverse=True)

    print "    - Top 5 vpub_old (shielded)"
    for pair in sorted_xsent[:5]:
        print "         -" + str(pair[0]) + " " + str(pair[1])
    print "    -  Total shielded " + str(sum(addrxSent.values()))
    print "    -  Intersected " + str(len(yazSent))
    print "    - Top 5 vpub_new (deshielded)"
    for pair in sorted_x[:5]:
        print "         -" + str(pair[0]) + " " + str(pair[1])
    print "    -  Total deshielded " + str(sum(addrx.values()))
    print "    -  Intersected " + str(len(yaz))


def convertAddressStats(path):
    print "loading vals"
    addrvals = pickle.load(open(path+'address_values.pkl', 'r'))
    # convert addr vals to an easy to use list
    addrDict = {}
    count = 0
    addrvsize = len(addrvals)
    print "creating dict from them"
    for row in addrvals:
        if count % 10000 == 0:
            print str(count) + "/" + str(addrvsize)
        count += 1
        addr = ''
        if row['vo_pubkey'] != None:
            addr = str(row['vo_pubkey'])
        elif row['vi_address'] != None:
            addr = str(row['vi_addr'])

        if addr not in addrDict:
            try:
                addrDict[addr] = {
                    'recv': 0.0 if row["recv"] is None else row['recv'],
                    'sent': 0.0 if row["sent"] is None else row['sent'],
                    'current_value': 0.0 if row["current_value"] is None else float('{0:.8f}'.format(row["current_value"])),
                    'txs_sent': 0 if row["txs_sent"] is None else row['txs_sent'],
                    'txs_recv': 0 if row["txs_recv"] is None else row['txs_recv'],
                    'vins_count': 0 if row["vins_count"] is None else row['vins_count'],
                    'vouts_count': 0 if row["vouts_count"] is None else row['vouts_count'],
                    'no_txs_total': 0 if row["no_txs_total"] is None else row['no_txs_total'],
                    'coingens_recv': 0 if row["coingens_recv"] is None else row['coingens_recv'],
                    'pool_sent': 0.0 if row["pool_sent"] is None else row['pool_sent'],
                    'pool_recv': 0.0 if row["pool_recv"] is None else row['pool_recv'],
                }
            except Exception as e:
                print "Error occured during processing"
                print e
                print row
                sys.exit(1)
    # print "dumping to pickle"
    # pickle.dump(addrDict, open(path+"address_stats.pkl", "w"))
    return addrDict


def loadFromDisk():
    print "loading clusters"
    clusters = pickle.load(open('clusters/clusters.pkl', 'r'))
    print "done, now loading graph"
    graph = pickle.load(open("clusters/graph.pkl", "r"))
    print "finished"
    return clusters, graph


def whereDoTheseAddressBelong(addresses, clusters):
    found = {}
    addresses = list(set(addresses))
    for addr in addresses:
        count = -1
        for cluster in clusters:
            count += 1
            if addr in cluster:
                #print "found address in cluster " + str(count) + ", size :" + str(len(cluster))
                if count not in found:
                    found[count] = []
                found[count].append(addr)
    print("addresses we found in the following clusters")
    print(found)
    return found


def main():
    sc, sql_sc = hf.initiateSpark()
    dft, dfb, dfc, dfvi, dfvo, dfvj = hf.loadDataframesFromParquet(sql_sc, conf.pathresearch)
    txs = generateSmallTXlist(dft, dfvi, dfvo, dfvj)
    clusters, graph, distinctnodeandtxs = generateClusters(txs)
    addressStats = convertAddressStats(conf.pathresearch)
    clustersWithStats = clusterStats(txs, clusters, graph, addressStats, distinctnodeandtxs, True, conf.pathresearch)
    writeToDisk(clusters, graph, clustersWithStats, conf.pathresearch)
    print("Clustering script complete")


def topKey(clusterswithstatsx, keyToSortBy, numberOfTopNWanted):
    print keyToSortBy
    topClustersByValue = sorted(clusterswithstatsx, key=lambda k: k["size"], reverse=True)
    total = sum([x[keyToSortBy] for x in topClustersByValue])
    if "distinct" in keyToSortBy:
        total = 2242848
    if keyToSortBy == "size":
        total = 1740378
    # for dit in topClustersByValue[:numberOfTopNWanted]:
    #     if dit['number'] in clusterTags and clusterTags[dit['number']] != "":
    #         print clusterTags[dit['number']] + " : " + str(dit[keyToSortBy])
    #     else:
    #         print str(dit['number']) + " : " + str(dit[keyToSortBy])
    print "Total  : " +  str(total)
    print "top 5  : " +  str( (float(sum([x[keyToSortBy] for x in topClustersByValue[:5]] )) / float(total) * 100))
    print "top 10 : " +  str( (float(sum([x[keyToSortBy] for x in topClustersByValue[:10]])) / float(total) * 100))
    print "top 25 : " +  str( (float(sum([x[keyToSortBy] for x in topClustersByValue[:25]])) / float(total) * 100))
    print "top 30 : " +  str( (float(sum([x[keyToSortBy] for x in topClustersByValue[:30]])) / float(total) * 100))
    print "all    : " +  str( (float(sum([x[keyToSortBy] for x in topClustersByValue])) / float(total) * 100))


def heuristic1stats():
    clustersWithStats = pickle.load(open("clustersWithStats.pkl", "r"))
    clustersWithStats2 = [clustersWithStats[k] for k in clustersWithStats]
    clustersWithStats = clustersWithStats2
    keys = clustersWithStats[0].keys()
    for key in keys:
        if key in ['cluster', 'miners','founders', 'pools', 'number', 'FoundersHeuristic',  'MinersHeuristic', 'founders', 'distinct_txs']:
            continue
        topKey(clustersWithStats, key, 10)
        print ""


if __name__ == '__main__':
    main()
