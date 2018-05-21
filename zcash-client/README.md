# Zcash Docker container

A Docker container running Zcash as a service and exposing the REST API.

## Prerequisites

Install [Docker][docker], e.g. on Debian/Ubuntu based systems

    sudo apt install docker.io

## Configuration

- Rename `docker/zcash.conf.backup` to `docker/zcash.conf`
- Change the 'rpcuser' and 'rpcpassword'
- Modify `docker/zcash.conf` according to your environment
(see [doc][zcash-conf]).


## Usage

Building the docker container (latest tagged GitHub version of Zcash):

    docker build -t zcash .

Starting the container:

    docker start zcash


# Exposed JSON RPC PORT

By default this program will expose the RPC port `8331` of the Zcash node to the local machine running docker.

Example of interaction using Pythons `python-bitcoinrpc` is below

    from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
    rpcuser="MyUserName"
    rpcpassword="MyPassword"
    rpcport=8331
    rpc_connection = AuthServiceProxy("http://%s:%s@127.0.0.1:%i/" % (rpcuser, rpcpassword, rpcport))
    rpc_connection.getinfo()

[zcash-conf]: https://github.com/zcash/zcash/blob/master/contrib/debian/examples/zcash.conf
