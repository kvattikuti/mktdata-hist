Pulls down historical market data (daily quotes) for Equities from Yahoo Finance and saves to a postgres database.

Build:

mkdir mktdatahst

cd mktdatahst

export GOPATH=`pwd`

mkdir src

go get github.com/lib/pq

go install github.com/lib/pq

go get github.com/kvattikuti/mktdata-hist

go install github.com/kvattikuti/mktdata-hist

Run:

cp src/github.com/kvattikuti/mktdata-hist/config.json bin/

Modify configuration for symbols, years and database url

cd bin

./mktdata-hst
