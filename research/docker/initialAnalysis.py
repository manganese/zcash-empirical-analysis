'''
Initial Analysis script, results are saved to the /root/research docker directory 
- Outputs a text file which includes the following results 
    - Total number of blocks
    - Transactions & types (shielded, deshielded, transparent, mixed, private)
'''

import helpfulFunctions as hf
import config as conf

def main():
    print("Generating initial statistics")
    sc, sql_sc = hf.initiateSpark()
    dft, dfb, dfc, dfvi, dfvo, dfvjx = hf.loadDataframesFromParquet(sql_sc, conf.pathresearch)
    print "Performing initial analysis"
    results = 'Initial Analysis : \n'
    # count blocks
    print "Obtaining Block counts"
    blocknum = dfb.count()
    results += 'Blocks: ' + str(blocknum) + '\n'
    # count transactions
    total = dft.count()
    print "Obtaining transaction statistics"
    results += 'Transactions: ' + str(total) + '\n'
    # types of txs
    types = [(r['t_type'], r['count']) for r in dft.groupBy('t_type').count().collect()]
    results += "Transactions by type \nType \tAmount \tPercentage \n"
    for type in types:
        results += type[0] + ' \t' + str(type[1]) + '\t' + ('%.2f' % (float(type[1])/float(total)*100)) + '\n'
    # write results to disk
    print "Writing results to file %s" % "/root/research/basic_analysis.txt"
    resultsfile = open(conf.pathresearch+"initial_analysis.txt", "w")
    resultsfile.writelines(results)
    resultsfile.close()
    print(results)
    print "Complete"


if __name__ == '__main__':
    main()
