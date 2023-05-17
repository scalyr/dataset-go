#!/usr/bin/env bash

# build example from README.md
cd ../examples/readme;
pwd;

# build example
rm -rfv readme
go build -o readme
ls -l readme

# try different domains
# https://badssl.com
for domain in https://expired.badssl.com https://wrong.host.badssl.com https://self-signed.badssl.com; do
  echo "Trying domain: ${domain}";
  export SCALYR_SERVER="${domain}";
  export SCALYR_WRITELOG_TOKEN="aaa";
  ./client
done;
