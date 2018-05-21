'''
d8888888P  a88888b.  .d888888  .d88888b  dP     dP
     .d8' d8'   `88 d8'    88  88.    "' 88     88
   .d8'   88        88aaaaa88a `Y88888b. 88aaaaa88a
 .d8'     88        88     88        `8b 88     88
d8'       Y8.   .88 88     88  d8'   .8P 88     88
Y8888888P  Y88888P' 88     88   Y88888P  dP     dP

This script parses the node data into dataframe files, removing the need to use postgres

'''

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import config as conf
import table_classes
import sys
from socket import error as socket_error
import pyspark.sql.functions as func
import helpfulFunctions as hf
from decimal import *
getcontext().prec = 8


def checkZcashCLI():
    """
    Checks if zcashd is running
    Prints stuff, returns nothing or exits the program
    """
    print("Setting environment variable to mainnet")
    try:
        proxy = rpcConnection()
        info = proxy.getinfo()
        print("zcashd running")
        print("Total blocks in Zcash : ", info["blocks"])
        print("Current relay fee: ", str(info["relayfee"]))
        print("Current transaction fee: ", str(info["paytxfee"]))
        print("Version: ", str(info["version"]))
    except ValueError as e:
        print("Error parsing configuration file")
        print(e.message)
        print("Exiting")
        sys.exit(1)
    except socket_error as serr:
        if serr.errno != errno.ECONNREFUSED:
            # different error
            raise serr
        else:
            # connection refused
            print("ERROR: Connection refused")
            print("Likely zcashd is not running")
            print("Please run zcashd or check config")
            print("Exiting...")
            sys.exit(1)


def rpcConnection():
    rpc_connection = AuthServiceProxy("http://%s:%s@zcash:%i/" % (conf.RPC_USER, conf.RPC_PASSWORD, conf.ZCASH_RPC_PORT))
    # test it
    try:
        info = rpc_connection.getinfo()
    except JSONRPCException as e:
        print "Authentication error " + str(e)
        print "Exiting program "
        sys.exit(1)

    return rpc_connection


def getType(tx):
    '''
    Identifies the transaction type, there are 5 types
    - minerReward:  Only for newly generated coins
    - public:       public->public
    - shielded:     public->shielded
    - deshielded:   shielded->public
    - private:      shielded->shielded
    :param tx: transaction as dictionary
    :return: type of transaction
    '''
    if (len(tx["vin"])) == 1 and tx["vin"][0].has_key("coinbase"):
        print("Type: Miner Reward")
        return table_classes.TX_TYPE_MINERREWARD
    elif len(tx["vin"]) and len(tx["vout"]) and not len(tx["vjoinsplit"]):
        print("Type: Public")
        return table_classes.TX_TYPE_PUBLIC
    elif len(tx["vin"]) and not len(tx["vout"]) and len(tx["vjoinsplit"]):
        print("Type: Shielded")
        return table_classes.TX_TYPE_SHIELDED
    elif len(tx["vin"]) and len(tx["vout"]) and len(tx["vjoinsplit"]):
        print("Type: Mixed")
        return table_classes.TX_TYPE_MIXED
    elif not len(tx["vin"]) and len(tx["vout"]) and len(tx["vjoinsplit"]):
        print("Type: Deshielded")
        return table_classes.TX_TYPE_DESHIELDED
    elif not len(tx["vin"]) and not len(tx["vout"]) and len(tx["vjoinsplit"]):
        print("Type: Private")
        return table_classes.TX_TYPE_PRIVATE
    else:
        print("Type: Unknown")
        return "UNKNOWN"


def fillDataFrames(sc, sql_sc, blocklimit):
    '''
    Updates the database with the latest blocks from the blockchain
    :param currentBlockHeight: the latest block height in the database, one to start from
    :return: the dataframes it created
    '''
    # Run RPC and get latest block height
    print("Using mainnet")
    print("Creating empty dataframes")
    dft, dfb, dfc, dfvi, dfvo, dfvj = hf.createEmptyDataframes(sc, sql_sc)
    print("Creating RPC connection")
    rpc = rpcConnection()
    print("Getting the latest block from the network")
    print("Collecting blocks from range 0 to " + str(blocklimit))
    objects = {
            "block":[],
            "transaction":[],
            "coingen":[],
            "vout": [],
            "vin": [],
            "vjoinsplit": []
    }
    for blockHeight in range(1, blocklimit+1, 1):
        print("Block " + str(blockHeight) +  "/" + str(blocklimit))
        # print("Block: " ,str(blockHeight))
        rpc = rpcConnection()
        block = rpc.getblock(str(blockHeight))
        #print("Block received, generating class")
        #print("Hash :" +  (str(block["hash"])))
        previousblockhash = ""
        if "previousblockhash" in block.keys():
            previousblockhash = block["previousblockhash"]
        nextblockhash = ""
        if "nextblockhash" in block.keys():
            nextblockhash = block["nextblockhash"]

        blockz = [
            block["height"],
            block["hash"],
            block["size"],
            block["version"],
            # block["merkleroot"],
            list(block["tx"]),
            block["time"],
            # block["nonce"],
            # block["difficulty"],
            # block["chainwork"],
            #solution = block["solution"],
            #bits = block["bits"],
            # block["anchor"],
            previousblockhash,
            nextblockhash,
        ]
        objects['block'].append(blockz)

        #print("Will add block once we have all transactions")
        # do the same for transactions
        # print(str(len(block["tx"])) +" transactions found in block")
        for tx_hash in block["tx"]:
            skip = 0
            rpc = rpcConnection()
            try:
                tx = rpc.getrawtransaction(tx_hash, 1)
            # TODO FIX THIS EXCEPTION
            except rpc.InvalidAddressOrKeyError:
                print("Could not find TX, no information")
                print("Setting value to False")
                skip=1

            tx_type = getType(tx)
            # fee = calculateFee(tx, tx_type)

            if skip:
                transaction = [
                    tx_hash,
                    # version=0,
                    # locktime=0,
                    block["hash"],
                    0,
                    0,
                    False,
                    0,
                    0,
                    0,
                    tx_type,
                ]
                objects['transaction'].append(transaction)
                continue
            else:
                transactionType = getType(tx)
                transaction = [
                    tx_hash,
                    # version = tx["version"],
                    # locktime = tx["locktime"],
                    tx["blockhash"],
                    tx["time"],
                    tx["blocktime"],
                    True,
                    len(tx["vin"]),
                    len(tx["vjoinsplit"]),
                    len(tx["vout"]),
                    tx_type,
                    # = Total VIN-VOUT
                    # fee= fee,
                    # total transparent output - output in the blockchain
                    # outputValue = Column('outputValue', Float)
                    # boolean, if contains shielded then true, if vjoinsplit then true
                    # shielded = Column('shielded', Boolean)
                    # value given into shielded
                    # shieldedValue = Column('shieldedValue', Float)
                    # computed from total VIN
                    # value = Column('value', Float)
                    #vin = simplejson.dumps(tx["vin"],use_decimal=True),
                    #vjoinsplit = simplejson.dumps(tx["vjoinsplit"],use_decimal=True),
                    #vout = simplejson.dumps(tx["vout"],use_decimal=True),
                ]
            #print("Processed tx :", tx_hash)
            objects['transaction'].append(transaction)
            # now check the values to see if it's a coingen, vin, vout and vjoinsplit fields
            all_src_addresses = []
            if len(tx["vin"]):
                for val in tx["vin"]:
                    if "coinbase" in val.keys():
                        # we found a coingen transaction, create the object and add it
                        coingen = [
                            tx_hash,
                            val["coinbase"],
                            val["sequence"]
                        ]
                        objects['coingen'].append(coingen)
                    else:
                        src_addresses = []
                        prevout_hash = val["txid"]
                        prevout_n = val["vout"]
                        # rpc = rpcConnection()
                        # try:
                        #     tempTX = rpc.getrawtransaction(prevout_hash, 1)
                        #     if "addresses" in tempTX["vout"][prevout_n]["scriptPubKey"]:
                        #         for addr in tempTX["vout"][prevout_n]["scriptPubKey"]["addresses"]:
                        #             src_addresses.append(addr)
                        #             all_src_addresses.append(addr)
                        #     src_addresses = list(set(src_addresses))
                        # except bitcoin.rpc.InvalidAddressOrKeyError:
                        #     print("Could not find TX, no information")
                        #     print("Setting value to False")
                        #     skip = 1
                        # else its a normal vin duh
                        vin = [
                            tx_hash,
                            prevout_hash,
                            prevout_n,
                            # val["sequence"],
                            # src_addresses, # this is done in a second spark script is that is faster
                        ]
                        # find the source address
                        objects['vin'].append(vin)
            allPubkeys = []
            if len(tx["vout"]):
                isVjoinsplit = False
                if len(tx["vjoinsplit"]) and len(tx["vin"])==0:
                    isVjoinsplit = True
                for val in tx["vout"]:
                    pubkey = []
                    if "addresses" in val["scriptPubKey"]:
                        for addr in val["scriptPubKey"]["addresses"]:
                            pubkey.append(addr)
                            allPubkeys.append(addr)
                    pubkey = list(set(pubkey))
                    vout = [
                        tx_hash,
                        val["value"],
                        # val["valueZat"],
                        val["n"],
                        pubkey,
                        # isVjoinsplit = isVjoinsplit,
                        #script =
                    ]
                    objects['vout'].append(vout)
            # all_src_addresses = list(set(all_src_addresses))
            allPubkeys = list(set(allPubkeys))
            if len(tx["vjoinsplit"]):
                for val in tx["vjoinsplit"]:
                    # type of vjoinsplit
                    # t-z
                    # z-t
                    # t-z
                    # don't use the crappy all_src_address value
                    dest_addr = []
                    if len(tx["vout"])==0 and len(tx["vin"])==0 and Decimal(val["vpub_new"])>0.0:
                        # this is change
                        dest_addr = []
                    elif len(tx["vout"])>0 and len(tx["vin"])==0 and Decimal(val["vpub_new"])>0.0:
                        dest_addr=allPubkeys

                    vjoinsplit = [
                        tx_hash,
                        val["vpub_old"],
                        val["vpub_new"],
                        # anchor = val["anchor"],
                        # nullifiers = list(val["nullifiers"]),
                        # commitments = list(val["commitments"]),
                        # onetimePubKey = val["onetimePubKey"],
                        # randomSeed = val["randomSeed"],
                        # macs = list(val["macs"]),
                        all_src_addresses,
                        dest_addr,
                    ]
                    objects['vjoinsplit'].append(vjoinsplit)
        dft, dfb, dfc, dfvi, dfvo, dfvj = hf.populateDataframe(sql_sc, objects, dft, dfb, dfc, dfvi, dfvo, dfvj)
        # print("Block complete")
    dfvi = hf.getInputAddresses(dfvi, dfvo)
    dfvj = dfvj.withColumn("vj_vpub_old", func.round(dfvj["vj_vpub_old"], 8))
    dfvj = dfvj.withColumn("vj_vpub_new", func.round(dfvj["vj_vpub_new"], 8))
    dfvj = hf.getSourceDestinationAddress(sql_sc, dfvi, dfvo, dfvj)

    print("Processed all blocks. Closing program")
    return dft, dfb, dfc, dfvi, dfvo, dfvj


def main():

    print("Connecting to Zcash node")
    sc, sql_sc = hf.initiateSpark()
    path = conf.pathresearch
    checkZcashCLI()
    blocklimit = conf.blockheight
    rpc = rpcConnection()
    latestBlock = int(rpc.getblockcount())
    if blocklimit > latestBlock:
        blocklimit = latestBlock
    dft, dfb, dfc, dfvi, dfvo, dfvj = fillDataFrames(sc, sql_sc, blocklimit)
    hf.saveDataframeToParquet(path, dft, dfb, dfc, dfvi, dfvo, dfvj)


if __name__ == '__main__':
    main()
