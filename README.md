Pulls down historical market data (daily quotes) for Equities from Yahoo Finance and saves to a postgres database.

# Build
1. mkdir mktdatahst
1. cd mktdatahst
1. export GOPATH=`pwd`
1. mkdir src
1. go get github.com/lib/pq
1. go install github.com/lib/pq
1. go get github.com/kvattikuti/mktdata-hist
1. go install github.com/kvattikuti/mktdata-hist

# Database setup
```
create table if not exists daily_quotes_hst (symbol varchar(10), trade_dt date, open real, high real, low real, close real, volume integer, adj_close real);
```

# Run
1. cp src/github.com/kvattikuti/mktdata-hist/config.json bin/
1. edit config.json to specify , symbols, years and database url
1. cd bin
1. ./mktdata-hst

# Known Issues
When run on new year, logic will skip getting quotes for last day of the past year. As a workaround, delete all of the past year data and rerun the program.
