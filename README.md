# zcash-empirical-analysis

This is the source code used in the research paper:

**An Empricial Analysis of Anonymity in Zcash** *George Kappos, Haaroon Yousaf, Mary Maller, Sarah Meiklejohn, 27th USENIX Security Symposium (2018, to appear),* https://arxiv.org/abs/1805.03180


Please read this `README.md` from start to finish before attempting the analysis.

# Prequisites

- Docker
- At least 3x storage space of the current blockchain


# Installation

- Clone this repository
- CD into the root of this repository `zcash-empirical-analysis`

## Configuration

### Config.py

The container directories of the data store directories must equal the value stores in the
`research/config.py` file. This is paramount, as this config is used during analysis

If you would like to increase the blockheight that this analysis is performed upon,
you can do so by changing the integer value of `blockheight` in the `research/docker/config.py` file

### Data store directories

- Create three directories for each of the containers and add them to the `docker-compose.yml` file
    - `.zcash` : `zcash` container, stores raw blockchain data downloaded by the zcash node
    - `pgdata` :  `postgres` container, stores parsed zcash blockchain data
    - `research` : `research` and `postgres` container, stores created parquet files and analysis data

- The directories are used as volumes, they are mounted on their respective docker containers

            <local_directory>:<container_directory>

    e.g.

            /data/data1/zcash/.zcash:/root/.zcash

    `/data/data1/zcash/.zcash` is the `<local_directory>`, a folder on the host machine

    `/root/.zcash` is the `<container_directory>`, the above folder but mounted in the docker container

- **Do not** change the container directory, simply change the `<local_directory>` as required

### Zcash Client

- Copy the `zcash-client/docker/zcash.conf.backup` file to the `.zcash` folder created earlier as `.zcash/zcash.conf`

- Configure the `zcash.conf` file by setting the `rpcuser` and `rpcpassword` variables

- Set the same values to the `RPC_USER` and `RPC_PASSWORD` variables in the `research/docker/config.py` and `zcashpostgres/docker/config.py`

- Set the variable `rpcclienttimeout` to `120`, e.g. `rpcclienttimeout=120`

## Address files

Sections (Heurustics and Clustering) within the analysis files require user address and tags in `csv`
format.  Due to the sensitivity of this data, we do not provide such
addresses.  Instead, the following files must be created in
`research/docker/addresses/` and, if desired, filled with addresses.  The
analysis can still be run without them being filled, but not if they are
not present.

All `csv` files expect the `;` as the delimeter. All addresses are expected
to be tAddresses (transparent addresses used in Zcash)

- `pool_addresses.csv`
    - The addresses and tags in this file should be related to mining pools,
      which can be found on mining pool websites
    - Double-column csv file with header `address;tag`, where each address has
      a single string as a tag (e.g., `tAddressA;poolX`)
    - Tags can be repeated but tAddresses must be unique

- `address_tags.csv`
    - The addresses and tags in this file can be related to any entity, and
      could be collected manually or scraped online
    - Double-column csv file with header `address;tag`, where each address has
      a single string as a tag (e.g., `tAddressA;exchangeX`)
    - Tags can be repeated but tAddresses must be unique

- `founders_addresses.csv`
    - These are the address of the Zcash founder, which can be found in the
      source code and whitepaper
    - Single-column csv file with header `address`, where each address must be
      on a separate line

# Setting up the containers

- Build and run the three containers by executing the following commands in the `zcash-empirical-analysis` folder

        docker-compose build
        docker-compose up -d

- This creates and runs the `zcash`, `zcashpostgres` and `research` containers with a network between them
so they can interact

**Container Interaction**
- To connect to the docker-node run the command below

            docker exec -it <containername> bash

- You must wait for the zcash node to sync to an appropriate height of the blockchain, you can check this by
    executing the command via the script

        ./zcash-client/docker/cli.sh <command> <arguments>

    which will execute the command on the zcash-cli interface and return the results

    **Note: If you get a `permission denied error` then `chmod +x` the script file**

- Once the zcash node is synced, the postgres database can be populated with data
    - Login into the `zcashpostgres` node using the docker command above and run the following
    - To setup the database and instantiate the tables *Note: This will erase all previous data*

            cd $SCRIPTS
            python setup.py
    - To parse the zcash node data into postgres

            cd $SCRIPTS
            python zcash_extraction.py

        *Note: This command will take 1 hour per 1,500 blocks.
        It will parse all the available data on the node.
        If re-run it will start from the last block commited in postgres
        If you cannot get a `connection refused` error, please check
        that the `rpcallowip` in the `zcash.conf` has been correctly set
        to the range used in the docker network*

- Once the above steps are complete you may continue

- First ensure the research container is running, this can be done by executing the command below
you should see a container called `research` with a running or up status

        docker-compose ps

    *Note: The research container may fail to run if the Apache Spark download fails, if this happens then check if the
    spark download link in the `research/Dockerfile` is active, if it isn't active then please replace this link with a
    url from the Apache Spark mirror*

- To do the analysis the container requires that the Zcash blockchain is
  parsed as Apache Spark Parquet files.  On docker this used method A, but
other methods would use less storage space.  Due to time constraints we could not
resolve the Spark issues.

    - Method A: slower, requires less RAM, however takes up more disk space.
        - First run the following in the `zcashpostgres` container, these will create `csv` files from Postgres and store them in
           `/root/research`

                psql -U postgres -d zcashdb -c "Copy (Select * From transactions) To STDOUT With CSV HEADER DELIMITER ';';" > /root/research/public.transaction.csv
                psql -U postgres -d zcashdb -c "Copy (Select * From vin) To STDOUT With CSV HEADER DELIMITER ';';" > /root/research/public.vin.csv
                psql -U postgres -d zcashdb -c "Copy (Select * From vout) To STDOUT With CSV HEADER DELIMITER ';';" > /root/research/public.vout.csv
                psql -U postgres -d zcashdb -c "Copy (Select * From vjoinsplit) To STDOUT With CSV HEADER DELIMITER ';';" > /root/research/public.vjoinsplit.csv
                psql -U postgres -d zcashdb -c "Copy (Select * From coingen) To STDOUT With CSV HEADER DELIMITER ';';" > /root/research/public.coingen.csv
                psql -U postgres -d zcashdb -c "Copy (Select * From blocks) To STDOUT With CSV HEADER DELIMITER ';';" > /root/research/public.block.csv
        - Next run the following in the `research` container, this will create `parquet` files from the `csv` files

                cd $SCRIPTS
                python createParquetFromCSV.py
        - Delete the `csv` files when the above command is complete

                rm -rf $RESEARCH/public.transaction.csv
                rm -rf $RESEARCH/public.vin.csv
                rm -rf $RESEARCH/public.vout.csv
                rm -rf $RESEARCH/public.vjoinsplit.csv
                rm -rf $RESEARCH/public.coingen.csv
                rm -rf $RESEARCH/public.block.csv

# Analysis

All research commands must be run in the `research` container.

All results are saved to the `/root/research` mounted folder.

## Initial research

### Initial Analysis

Generates statistics containing: total number of blocks, transactions &
Types (shielded, deshielded, transparent, mixed, private).  The result is saved to a text file `root/research/initial_analysis.txt`

    cd $SCRIPTS
    python initialAnalysis.py

### Address Statistics

Generates address-based statistics such as coins sent, coins received, current coins,
transactions sent and received, blocks mined, estimated amount in pool.

    cd $SCRIPTS
    python addressStatistics.py

These are saved in the file `addresses_values.csv`

The results are saved as a list of rows in a pickled file `address_values.pkl`

**Note: This script must be run *before* the address clustering as it is
dependent on the calculated address values**

The format of the rows in the list are as follows:

       Row(
                "address": "addressA"
                "pool_recv": 0.0,
                "pool_sent": 0.0,
                "coingens_recv": 0,
                "vouts_count": 0,
                "vins_count": 0,
                "txs_recv": 0,
                "txs_sent": 0,
                "recv": 0.0,
                "sent": 0.0,
                "no_txs_total": 0
        )

It also generates separate rich list txt files for the top 10 addresses that:
- sent coins - `rich_list_top_10_sent.csv`
- received coins - `rich_list_top_10_recv.csv`
- current balance - `rich_list_top_10_value.csv`


## Heuristics

The analysis consists of the files `heuristicsGraphs.py` and `plotGraphs.py` which create the
data for our results and plot the graphs of the paper respectively.

Our analysis consists of the graphs 2, 4, 5, 6, 8a, 8b, 8c, 9 as shown in the
paper as well as the heuristics 3, 4, 5

To run the analysis, login to the `research` container and run the following

    cd $SCRIPTS
    python heuristicsGraphs.py 2 4 5 6 8a 8b 8c 9 h3 h4 h5


These will plot and save the following graphs in the folder `$RESEARCH/Graphs`:

- Graph 2 : `$RESEARCH/Graphs/TransactionTypes.pdf`

- Graph 4 : `$RESEARCH/Graphs/TotalValueOverTime.pdf`

- Graph 5 : `$RESEARCH/Graphs/Deposits-Withdrawals.pdf`

- Graph 6 : `$RESEARCH/Graphs/DepositsPerIdentity.pdf`

- Graph 8a : `$RESEARCH/Graphs/WithdrawalsPerIdentityNoHeuristic.pdf`

- Graph 8b : `$RESEARCH/Graphs/WithdrawalsPerIdentityHeuristicF.pdf`

- Graph 8c : `/$RESEARCH/Graphs/WithdrawalsPerIdentityHeuristicFM.pdf`

- Graph 9 : `$RESEARCH/Graphs/FounderCorrelation.pdf`

The user can choose which graphs to produce and which heuristics to run.

This is done by by specifying command line arguments to the `script.py` file

The valid arguments are: `2 4 5 6 8a 8b 8c 9 h3 h4 h5`
e.g. If the user to produce the graphs 4 and 5 run heuristic h5, they would run

    cd $SCRIPTS
    python heuristicsGraphs.py 4 5 h5

The results of heuristics 3, 4, 5 are stored in files `founders_heuristic_addresses.csv`,
`miners_heuristic_addresses.csv` and `heuristic5.txt`:

- `founders_heuristic_addresses.csv`
        - single column csv file with the header 'address'
        - each address is on a separate line
        - these addresses are the founders addresses from the founders heuristic

- `miners_heuristic_addresses.csv`
        - single column csv file with header 'address'
        - each address is on a separate line
        - these addresses are the miner addresses from the mining heuristic,

- `heuristic5.txt`
        - Details about results in heuristic 5

- `miners_addresses.csv`
        - single column csv file with header 'address'
        - where each address is on a separate line and associated with a miner

## Address Clustering and Tagging

**Note: This task is dependent on the following files**

These files must be provided by the user, the details of them can be found above

- Pool Addresses `pool_addresses.csv`
- Founders Addresses `founders_addresses.csv`
- Address tags `address_tags.csv`

These files are automatically generated by the following tasks

- Address Statistics `address_values.pkl`, Generated in task Address Statistics
- Miners Addresses `miners_addresses.csv`, Generated in task Heuristic
- Founders Heuristic `founders_heuristic_addresses.csv`, Generated in task Heuristic
- Miners Heuristic  `miners_heuristic_addresses.csv`, Generated in task Heuristic


The clustering process builds, tags and produces statistics for the groups of addresses
that have been used as inputs in the transaction.
This is done by creating addresses as nodes in a graph, and joining them via edges if
they had been used as inputs in a transaction. This is done from the first transaction
to the last (depending on the latest block height).
Once complete, the code extracts the clusters (connected components) of the graph and
computes statistics based on the addresses contained within the clusters.
Each address is associated with only one cluster.
You may find that there are many clusters which have only one address, which
means that this address had only ever been used as the sole input in a transaction.

To run the address clustering, execute the following in the `research` container

        cd $SCRIPTS
        python heuristic1Clustering.py


The statistics produced are : cluster size (starting from 0), clustered addresses, tags for pools, miners, founders,
miners heuristic and founders heuristic, size, coingens sent and received,
pool sent and received, total amount of coins send and received, transactions
sent and received, number of distinct transactions within that cluster
and tags.

This script outputs the results in the following files:

- `README-heuristic1.md` - statistics about the clusters and details
about the addresses
- `cluster_stats.pkl` - a python pickled dictionary containing the
clusters ranked in order by size, largest starting at 0, and the above
statistics in dictionary
- `clusters_graph.pkl` - the graph used to generate the clusters
- `clusters_stats.csv` - statistics but in `csv` format


## Running analysis via runAll.sh

You can run all of the scripts within the analysis
by executing the following in the `research` container

    cd $SCRIPTS
    ./runAll.sh


# Updating
- In the future you may want to re-run the experiment using a more up-to-date blockchain
- To do this do the following
    - Ensure the `zcash` container is synced to the latest block height
    - Re-run the configuration steps, editing the `research/docker/config.py` file with a higher block height
    - Re-run the analysis, you may keep the manual csv files you previously created


# Appendix

## Other ways to generate the data

Below are two alternate methods that can be used to generate the `parquet` files.
These require much less hard drive space, but use much more RAM.
There are issues that prevent the files being created due to the connection
between the spark workers in docker. Thus, these can be run outside of docker
fine, but has issues if run within docker.

- Method B: faster but requires much more RAM on the machine, however due to issues with spark in docker and time
    constraints we were unable to get this to run on large block sizes. This runs fine if spark is run outside of docker.

    - Run the following in the `research` container, this will create Apache Spark `parquet` files directly from the postgres database and
        store them in `/root/research` which is mapped to the folder above.

                cd $SCRIPTS
                python createParquetDirectlyFromPostgres.py
        *Note: This command can fail if there is not enough memory for the Spark instance*

- Method C: faster as it parses data from the node directly into memory, then
  into spark files. We found issues
    running this on large block sizes, but this runs well outside of a docker container.

    - Run the following in the `research` container, this will create Apache Spark `parquet` files directly from the zcash node and
        store them in `/root/research` which is mapped to the folder above.

                cd $SCRIPTS
                python parseFromNode.py
        *Note: This command can fail if there is not enough memory for the Spark instance*

- Method D: A much faster method and space-efficient method to parse the
 blockchain is to directly read the raw block data (blk0*.dat files)
 into parquet. This has been left for future work.



[postgres95]: https://hub.docker.com/_/postgres/
