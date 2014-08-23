Pulls down historical market data (daily quotes) for Equities from Yahoo Finance.

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
cd bin
./mktdata-hst
