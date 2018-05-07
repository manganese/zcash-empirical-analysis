#!/bin/sh
cd $SCRIPTS
echo "Running initial_analysis.py"
python initialAnalysis.py
echo "Running address_statistics.py"
python addressStatistics.py
echo "Running heuristicsGraphs.py 2 4 5 6 8a 8b 8c 9 h3 h4 h5"
python heuristicsGraphs.py 2 4 5 6 8a 8b 8c 9 h3 h4 h5
echo "Running heuristic1Clustering.py 2 4 5 6 8a 8b 8c 9 h3 h4 h5"
python heuristic1Clustering.py
echo "Complete!"