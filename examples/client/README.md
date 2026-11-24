# Simple Client

This folder contains simple client that is able to send event to the DataSet.

## How To Run It

1. Set environmental variable for server - `export SCALYR_SERVER=""`
2. Set environmental variable for token - `export SCALYR_WRITELOG_TOKEN=""`
3. Build client: `go build -o client`
4. Run the client: `./client`

Optional: `./client -param VALUE` adds `"param": "VALUE"` to every event, so the events of one run
can be found with a query.

## End-to-End Check

`scripts/run-example-client.sh` runs this client with a unique `-param`, then uses the
[query API](https://app.scalyr.com/help/api#query) to check that all its events were stored. It needs
`SCALYR_SERVER`, `SCALYR_WRITELOG_TOKEN` and `SCALYR_READLOG_TOKEN`; see the script's header for the
optional settings. The `DataSet E2E` workflow runs it with repository secrets of the same names.
