package main

import "encoding/json"
import "io/ioutil"
import "fmt"
import "net/http"
import "net/url"
import "strings"
import "sync"
import (
	"database/sql"
	"github.com/lib/pq"
)
import "strconv"
import "time"

type Response struct {
	Query struct {
		Results struct {
			DailyTicks []struct {
				Symbol   string `json:"Symbol"`
				Date     string `json:"Date"`
				Open     string `json:"Open"`
				High     string `json:"High"`
				Low      string `json:"Low"`
				Close    string `json:"Close"`
				Volume   string `json:"Volume"`
				AdjClose string `json:"Adj_Close"`
			} `json:"quote"`
		} `json:"results"`
	} `json:"query"`
}

type Symbol struct {
	Sym string `json:"symbol"`
}

type Config struct {
	API_URL        string   `json:"api_url"`
	YQL            string   `json:"yql"`
	YearEnding     int      `json:"year_ending"`
	YearBeginning  int      `json:"year_beginning"`
	Symbols        []Symbol `json:"symbols"`
	DatabaseDriver string   `json:"database_driver"`
	DatabaseURL    string   `json:"database_url"`
}

func loadConfig() *Config {
	bytes, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}

	var conf = Config{}
	if err := json.Unmarshal(bytes, &conf); err != nil {
		panic(err)
	}

	return &conf
}

func generateRequestURLs(apiUrl string, yql string, symbols []Symbol, yearBegin int, yearEnd int,
	pastYearStmt *sql.Stmt, currYearStmt *sql.Stmt) <-chan string {
	out := make(chan string)
	go func() {
		today := time.Now()
		for i := 0; i < len(symbols); i++ {
			for year := yearBegin; year <= yearEnd ; year++ {
				if year == today.Year() {
					nextHstDt := getNextHstDate(currYearStmt, symbols[i].Sym, today.Year())
					if nextHstDt.Day() < today.Day() {
						fmt.Printf("Preparing API call for current year, getting historical quotes for %s since %d-%d-%d\n",
							symbols[i].Sym, nextHstDt.Year(), int(nextHstDt.Month()), nextHstDt.Day())
						out <- formatRequestURL(apiUrl, yql, symbols[i].Sym, nextHstDt.Year(), int(nextHstDt.Month()),
							nextHstDt.Day(), int(today.Month()), today.Day())
					} else {
						fmt.Printf("Skipping API call, historical quotes for %s exist for the year %d\n",
							symbols[i].Sym, year)
					}
				} else if !quotesExistForPastYear(pastYearStmt, symbols[i].Sym, year) {
					fmt.Printf("Preparing API call, historical quotes for %s exist for the year %d\n",
						symbols[i].Sym, year)
					out <- formatRequestURL(apiUrl, yql, symbols[i].Sym, year, 1, 1, 12, 31)
				} else {
					nextHstDt := getNextHstDate(currYearStmt, symbols[i].Sym, year)
					if nextHstDt.Day() < 31 && nextHstDt.Year() == year {
						fmt.Printf("Preparing API call for the year %d, getting historical quotes for %s since %d-%d-%d\n",
							year, symbols[i].Sym, nextHstDt.Year(), int(nextHstDt.Month()), nextHstDt.Day())
						out <- formatRequestURL(apiUrl, yql, symbols[i].Sym, nextHstDt.Year(), int(nextHstDt.Month()),
							nextHstDt.Day(), int(today.Month()), today.Day())
					} else {
						fmt.Printf("Skipping API call, historical quotes for %s exist for the year %d\n",
							symbols[i].Sym, year)
					}
				}
			}
		}
		close(out)
	}()
	return out
}

func formatRequestURL(apiUrl string, yql string, symbol string, year int, startMonth int, startDay int, endMonth int, endDay int) string {

	//start_dt := strings.Join([]string{ strconv.Itoa(year), strconv.Itoa(startMonth), strconv.Itoa(startDay)}, "-")
	//end_dt := strings.Join([]string{ strconv.Itoa(year), strconv.Itoa(endMonth), strconv.Itoa(endDay)}, "-")

	symYql := fmt.Sprintf(yql, symbol, year, startMonth, startDay, year, endMonth, endDay)
	v := url.Values{}
	v.Add("q", symYql)
	v.Add("format", "json")
	v.Add("env", "store://datatables.org/alltableswithkeys")
	v.Add("callback", "")
	reqParamsString := v.Encode()
	//??? - why doesn't Go encode spaces as %20?
	reqParamsStringEncoded := strings.Replace(reqParamsString, "+", "%20", -1)
	return apiUrl + reqParamsStringEncoded
}

func quotesExistForPastYear(stmt *sql.Stmt, symbol string, year int) bool {

	start_dt := strings.Join([]string{strconv.Itoa(year), "01", "01"}, "-")
	end_dt := strings.Join([]string{strconv.Itoa(year), "12", "31"}, "-")
	rows, err := stmt.Query(symbol, start_dt, end_dt)
	defer rows.Close()
	if err != nil {
		fmt.Printf("error running select query for symbol and year %s\n", err.Error())
		return false
	}
	return rows.Next()
}

func getNextHstDate(stmt *sql.Stmt, symbol string, year int) time.Time {

	start_dt := strings.Join([]string{strconv.Itoa(year), "01", "01"}, "-")
	var nextHstDt time.Time
	err := stmt.QueryRow(symbol, start_dt).Scan(&nextHstDt)
	if err != nil {
		return time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return nextHstDt
}

func getResponseBody(requestURLs <-chan string) <-chan []byte {

	//TODO: don't like error handling, is there a better way?
	out := make(chan []byte)
	go func() {
		for url := range requestURLs {
			fmt.Printf("%s\n", url)
			resp, err := http.Get(url)
			if err != nil {
				fmt.Printf("%s", err)
				out <- nil
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				out <- nil
			}
			out <- body
		}
		close(out)
	}()
	return out
}

func parseResponse(in <-chan []byte) <-chan (*Response) {
	out := make(chan (*Response))
	go func() {
		for responseBody := range in {
			res := Response{}
			if err := json.Unmarshal(responseBody, &res); err != nil {
				out <- nil
			}
			out <- &res
		}
		close(out)
	}()
	return out
}

func saveDailyQuotes(in <-chan (*Response), db *sql.DB) {
	for res := range in {
		if res != nil && len(res.Query.Results.DailyTicks) > 0 {
			fmt.Printf("Saving quotes for %s...\n", res.Query.Results.DailyTicks[0].Symbol)
			tx, err := db.Begin()
			if err != nil {
				fmt.Println("error creating db tx")
				return
			}
			stmt, err := tx.Prepare(pq.CopyIn("daily_quotes_hst", "symbol", "trade_dt", "open", "high", "low", "close", "volume", "adj_close"))
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for i := 0; i < len(res.Query.Results.DailyTicks); i++ {
				q := res.Query.Results.DailyTicks[i]
				_, err = stmt.Exec(q.Symbol, q.Date, q.Open, q.High, q.Low, q.Close, q.Volume, q.AdjClose)
				if err != nil {
					fmt.Printf("error saving quotes to db tx: %s\n", err.Error())
					break
				}
			}

			_, err = stmt.Exec()
			if err != nil {
				fmt.Printf("error saving quotes to db tx: %s\n", err.Error())
			}

			err = stmt.Close()
			if err != nil {
				fmt.Printf("error closing stmt: %s\n", err.Error())
			}

			err = tx.Commit()
			if err != nil {
				fmt.Printf("error commiting tx\n")
				return
			}
			fmt.Printf("Total daily ticks for %s : %d\n", res.Query.Results.DailyTicks[0].Symbol, len(res.Query.Results.DailyTicks))
		}
	}
}

func merge(cs ...<-chan []byte) <-chan []byte {
	var wg sync.WaitGroup
	out := make(chan []byte)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan []byte) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {

	// load configuration
	config := loadConfig()

	db, err := sql.Open(config.DatabaseDriver, config.DatabaseURL)
	if err != nil {
		fmt.Println("error connecting to db")
		return
	}
	defer db.Close()

	pastYearStmt, err := db.Prepare("select 1 from daily_quotes_hst where symbol = $1 and trade_dt >= $2 and trade_dt <= $3 limit 1")
	if err != nil {
		fmt.Println("error preparing statement")
		return
	}
	defer pastYearStmt.Close()

	currYearStmt, err := db.Prepare("select max(trade_dt)+1 from daily_quotes_hst where symbol = $1 and trade_dt >= $2")
	if err != nil {
		fmt.Println("error preparing statement")
		return
	}
	defer currYearStmt.Close()

	// generate request URLs to concurrently pull down historical prices from yahoo.finance
	in := generateRequestURLs(config.API_URL, config.YQL, config.Symbols, config.YearBeginning, config.YearEnding, pastYearStmt, currYearStmt)

	//distribute http calls across several routines
	r := merge(getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in),
		getResponseBody(in))

	p := parseResponse(r)
	saveDailyQuotes(p, db)

}
