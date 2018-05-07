#This script parses through the pools_addresses.csv you created and creates a python set of the addresses.
import config as conf

pools=set()
with open(conf.pathresearch+"pool_addresses.csv","r") as fr:
        for line in fr:
                if "address" not in line:
                        addr, name = line.replace("\n","").split(";")
                        pools.add(addr)