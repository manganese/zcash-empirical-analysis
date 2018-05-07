'''
d8888888P  a88888b.  .d888888  .d88888b  dP     dP
     .d8' d8'   `88 d8'    88  88.    "' 88     88
   .d8'   88        88aaaaa88a `Y88888b. 88aaaaa88a
 .d8'     88        88     88        `8b 88     88
d8'       Y8.   .88 88     88  d8'   .8P 88     88
Y8888888P  Y88888P' 88     88   Y88888P  dP     dP

This code extracts and parses node data into postgres

'''
import time
import errno
import os
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import config
import table_classes
import sys
from socket import error as socket_error, socket
import errno
from sqlalchemy import exc
import dbLib


def checkZcashCLI():
    """
    Checks if the Zcashd is running
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


def checkPostGreSQL():
    '''
    Checks if the postgresql server is up and running
    If not it just exits
    returns nothing
    '''
    try:
        # check if there is a server connection
        con, meta = dbLib.connect(db=config.POSTGRES_DB)
        # check if tables exist
        # check for table blocks and transactions
        if con.has_table("blocks") and con.has_table("transactions"):
            print("Table \'blocks\' and \'transactions\' are present")
        else:
            print("Table \'blocks\' or \'transactions\' is missing, please check database")
            print("Exiting")
            sys.exit(1)
        # print number of blocks present
        # print number of transactions present
    except exc.SQLAlchemyError as e:
        print("SQLAlchemy error: ", e)
        print("Exiting")
        sys.exit(1)


def rpcConnection():
    rpc_connection = AuthServiceProxy("http://%s:%s@zcash:%i/" % (config.RPC_USER, config.RPC_PASSWORD, config.ZCASH_RPC_PORT), timeout=120)
    # test it
    count = 0
    maxTries = 5
    while(count<maxTries):
        count += 1
        try:
            info = rpc_connection.getinfo()
            return rpc_connection
        except JSONRPCException as e:
            print "Authentication error " + str(e)
            print "Exiting program "
            sys.exit(1)
        except Exception as e:
            # apologies for bad exception catching
            print("Socket error when connecting to rpc")
            print(e)
            print("Waiting 5 seconds then trying again")
            time.sleep(5)
    print("Could not establish a connection")
    print("Exiting")
    sys.exit(1)



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
        # print("Type: Miner Reward")
        return table_classes.TX_TYPE_MINERREWARD
    elif len(tx["vin"]) and len(tx["vout"]) and not len(tx["vjoinsplit"]):
        # print("Type: Public")
        return table_classes.TX_TYPE_PUBLIC
    elif len(tx["vin"]) and not len(tx["vout"]) and len(tx["vjoinsplit"]):
        # print("Type: Shielded")
        return table_classes.TX_TYPE_SHIELDED
    elif len(tx["vin"]) and len(tx["vout"]) and len(tx["vjoinsplit"]):
        # print("Type: Mixed")
        return table_classes.TX_TYPE_MIXED
    elif not len(tx["vin"]) and len(tx["vout"]) and len(tx["vjoinsplit"]):
        # print("Type: Deshielded")
        return table_classes.TX_TYPE_DESHIELDED
    elif not len(tx["vin"]) and not len(tx["vout"]) and len(tx["vjoinsplit"]):
        # print("Type: Private")
        return table_classes.TX_TYPE_PRIVATE
    else:
        # print("Type: Unknown")
        return "UNKNOWN"


def updateDatabase(currentBlockHeight, latestBlock):
    '''
    Updates the database with the latest blocks from the blockchain
    :param currentBlockHeight: the latest block height in the database, one to start from
    :return: nothing
    '''
    # Run RPC and get latest block height
    print("Updating database")
    print("Using mainnet")
    print("Latest block is : "+ str(latestBlock))
    print("Collecting blocks from range " + str(currentBlockHeight) + " to " + str(latestBlock))
    for blockHeight in range(currentBlockHeight+1, latestBlock+1, 1):
        print("Block " + str(blockHeight) +  "/" + str(latestBlock))
        objects = []
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
        databaseBlock = table_classes.Blocks(
            height = block["height"],
            hash = block["hash"],
            size = block["size"],
            version = block["version"],
            merkleroot = block["merkleroot"],
            tx = list(block["tx"]),
            time = block["time"],
            nonce = block["nonce"],
            #solution = block["solution"],
            #bits = block["bits"],
            difficulty = block["difficulty"],
            chainwork = block["chainwork"],
            anchor = block["anchor"],
            previousblockhash = previousblockhash,
            nextblockhash = nextblockhash,
        )
        objects.append(databaseBlock)
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
                transaction = table_classes.Transactions(
                    tx_hash=tx_hash,
                    version=0,
                    locktime=0,
                    blockhash=block["hash"],
                    time=0,
                    blocktime=0,
                    type = tx_type,
                    #vin="",
                    #vjoinsplit="",
                    #vout="",
                    valid=False
                )
                objects.append(transaction)
                continue
            else:
                # transactionType = getType(tx)
                transaction = table_classes.Transactions(
                    tx_hash = tx_hash,
                    version = tx["version"],
                    locktime = tx["locktime"],
                    blockhash = tx["blockhash"],
                    time = tx["time"],
                    blocktime = tx["blocktime"],
                    tx_vin_count = len(tx["vin"]),
                    tx_vj_count = len(tx["vjoinsplit"]),
                    tx_vout_count = len(tx["vout"]),
                    type = tx_type,
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
                    valid = True
                )
            #print("Processed tx :", tx_hash)
            objects.append(transaction)
            # now check the values to see if its a coingen, vin, vout and vjoinsplit fields
            all_src_addresses = []
            if len(tx["vin"]):
                for val in tx["vin"]:
                    if "coinbase" in val.keys():
                        # we found a coingen transaction, create the object and add it
                        coingen = table_classes.Coingen(
                            tx_hash = tx_hash,
                            coinbase = val["coinbase"],
                            sequence = val["sequence"]
                        )
                        objects.append(coingen)
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
                        vin = table_classes.Vin(
                            tx_hash = tx_hash,
                            prevout_hash = prevout_hash,
                            prevout_n = prevout_n,
                            sequence = val["sequence"],
                            address = src_addresses, # this is done in a second spark script is that is faster
                            # script = INCOMPLETE
                        )
                        # find the source address
                        objects.append(vin)
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
                    vout = table_classes.Vout(
                        tx_hash =  tx_hash,
                        value = val["value"],
                        valueZat = val["valueZat"],
                        tx_n = val["n"],
                        pubkey = pubkey,
                        isVjoinsplit = isVjoinsplit,
                        #script =
                    )
                    objects.append(vout)
            # all_src_addresses = list(set(all_src_addresses))
            allPubkeys = list(set(allPubkeys))
            if len(tx["vjoinsplit"]):
                for val in tx["vjoinsplit"]:
                    # type of vjoinsplit
                    # t-z
                    # z-t
                    # t-z
                    # dont use the crappy all_src_address value
                    dest_addr = []
                    if len(tx["vout"])==0 and len(tx["vin"])==0 and float(val["vpub_new"])>0.0:
                        # this is change
                        dest_addr = []
                    elif len(tx["vout"])>0 and len(tx["vin"])==0 and float(val["vpub_new"])>0.0:
                        dest_addr=allPubkeys

                    vjoinsplit = table_classes.Vjoinsplit(
                        tx_hash = tx_hash,
                        vpub_old = val["vpub_old"],
                        vpub_new = val["vpub_new"],
                        anchor = val["anchor"],
                        nullifiers = list(val["nullifiers"]),
                        commitments = list(val["commitments"]),
                        onetimePubKey = val["onetimePubKey"],
                        randomSeed = val["randomSeed"],
                        macs = list(val["macs"]),
                        dest_address = dest_addr,
                        src_address = all_src_addresses
                    )
                    objects.append(vjoinsplit)
        dbLib.addObjects(objects)
        # print("Block complete")
    print("Processed all blocks. Closing program")


def checkStatus():
    """
    Checks if the ZCash process and PostGreSQL databases are active
    """
    checkZcashCLI()
    checkPostGreSQL()
    currentBlockHeight = dbLib.getLatestBlockheight()
    rpc = rpcConnection()
    latestBlock = int(rpc.getblockcount())
    print("Getting the latest block from the network")
    print("Current block height in database %s", str(currentBlockHeight))
    print("Current block height in database %s", str(latestBlock))
    print("Updating database")
    updateDatabase(currentBlockHeight, latestBlock)


if __name__ == '__main__':
    """
    Initiate program
    """
    print("\n****************************************************")
    print("****************************************************")
    print("d8888888P  a88888b.  .d888888  .d88888b   dP     dP")
    print("     .d8' d8'   `88 d8'    88  88.    \"' 88     88")
    print("   .d8'   88        88aaaaa88a `Y88888b.  88aaaaa88a")
    print(" .d8'     88        88     88        `8b  88     88")
    print("d8'       Y8.   .88 88     88  d8\'   .8P 88     88")
    print("Y8888888P  Y88888P\'88     88   Y88888P   dP     dP")
    print("****************************************************")
    print("****************************************************\n")
    print("******** Zcash blocks and transaction updater *********")
    print("INFO: \n")
    print("> This program will check the local PostGreSQL database")
    print("> and update the blocks and respective transactions.")
    print("> Once complete the program will close.")
    print("> This process block at a rate of hour/15k blocks .\n")
    checkStatus()
