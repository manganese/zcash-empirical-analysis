'''
This script parses through the founders_addresses.csv you created and creates a python set of the addresses.
'''
import config as conf

trivialFounders=set()
with open(conf.pathresearch+"founders_addresses.csv","r") as fr:
        for line in fr:
                if line!="address\n":
                        trivialFounders.add(line.replace("\n","")) 