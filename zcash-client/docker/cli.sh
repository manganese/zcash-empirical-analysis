[ "$#" -lt 1 ] && printf "Please provide at least one arguments for the zcash-cli\n" && exit
for var in "$@"; do x="$x $var"; done
# echo "executing command on server, zcash-cli $x"
docker exec zcash bash -c "zcash-cli "$x