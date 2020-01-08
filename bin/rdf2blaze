#!/usr/bin/env bash

set -euf -o pipefail

if [ $# -ne 1 ]; then
    >&2 echo "Usage: $0 <path-to-rdf>"
    >&2 echo "Where path-to-rdf is a path to an RDF file."
    exit 1
fi

rdf_path=$1

if [ ! -f "$rdf_path" ]; then
    >&2 echo "Path $rdf_path does not appear to be a file."
    exit 1
fi

blazegraph_host=localhost
blazegraph_port=8889
blazegraph_base_url="http://$blazegraph_host:$blazegraph_port/bigdata"
blazegraph_load_url="$blazegraph_base_url/sparql"
blazegraph_query_url="$blazegraph_base_url/#query"
blaze_status_url="$blazegraph_base_url/status"

echo "Starting Blazegraph docker container."
blazegraph_container_id=$(docker run -d -p $blazegraph_port:8080 lyrasis/blazegraph:2.1.5)
trap 'echo "Stopping Blazegraph docker container $blazegraph_container_id" && docker kill $blazegraph_container_id' EXIT
echo "Blazegraph container running: $blazegraph_container_id"
echo "Waiting for service to be available..."
n=0
until [ $n -ge 10 ]; do
    echo "Waiting for $blaze_status_url..."
    sleep 5
    set +e
    curl "$blaze_status_url" > /dev/null 2>&1
    curl_ret=$?
    set -e
    if [[ $curl_ret == 0 ]]; then
        echo "$blaze_status_url is up."
        break
    fi
    n=$[$n+1]
done
if [ $n -ge 10 ]; then
    >&2 echo "$blaze_status_url did not come up!"
    exit 1
fi

echo "Loading $rdf_path using $blazegraph_load_url"
load_stats=$(curl -X POST -H "Content-Type: application/rdf+xml" -d @"$rdf_path" "$blazegraph_load_url" 2>/dev/null)
echo "Load stats: $load_stats"
echo "Data available at $blazegraph_query_url"

echo "Blazegraph is running with data from $rdf_path."
echo
echo "Query UI is available at $blazegraph_query_url"
echo
echo "Hit CTRL-C to exit."
read -r -d '' _ </dev/tty

