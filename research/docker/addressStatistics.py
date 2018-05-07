'''
Generated address based statistics such as coins sent, coins received, currnet coins,
transactions sent and received, blocks mined, estimated amount in pool.
'''

import helpfulFunctions as hf
import config as conf

def main():
    print("Generating address statistics")
    sc, sql_sc = hf.initiateSpark()
    dft, dfb, dfc, dfvi, dfvo, dfvj = hf.loadDataframesFromParquet(sql_sc, conf.pathresearch)
    dfb.unpersist()
    hf.generateAllStats(sc, dfvi, dfvo, dfc, dft, dfvj, conf.pathresearch)


if __name__ == '__main__':
    main()