package main

import "encoding/json"
import "io/ioutil"
import "fmt"
import "net/http"
import "net/url"
import "strings"
import "sync"

type Response struct {
	Query struct {
		Results struct {
			DailyTicks [] struct {
				Symbol string		`json:"Symbol"`
				Date string			`json:"Date"`
				Open string			`json:"Open"`
				High string			`json:"High"`
				Low string			`json:"Low"`
				Close string		`json:"Close"`
				Volume string		`json:"Volume"`
				AdjClose string		`json:"Adj_Close"`

			}						`json:"quote"`
		}							`json:"results"`
	}								`json:"query"`
}

type Symbol struct {
		Sym string		`json:"symbol"`
}			

type Config struct {
	API_URL	string		`json:"api_url"`
	YQL string 			`json:"yql"`	
	YearEnding int 		`json:"year_ending"`
	YearBeginning int 	`json:"year_beginning"`
	Symbols []Symbol 	`json:"symbols"`
}

func loadConfig() (*Config) {
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

func generateRequestURLs(apiUrl string, yql string, symbols []Symbol, yearBegin int, yearEnd int) <-chan string {
	out := make(chan string)
	go func() {
		for i := 0; i < len(symbols); i++ {
			for year := yearEnd; year >= yearBegin; year-- {
				//TODO: output URLs to a channel
				out <- fmt.Sprintf("%s", formatRequestURL(apiUrl, yql, symbols[i].Sym, year))
			} 
		}
		close(out)
    }()
	return out
}

func formatRequestURL(apiUrl string, yql string, symbol string, year int) string {

	symYql := fmt.Sprintf(yql, symbol, year, year)
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

func getResponseBody(requestURLs <- chan string) <-chan []byte {

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

func parseResponse(in <- chan []byte) <- chan (* Response) {
	out := make(chan (* Response))
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

func saveDailyQuotes(in <- chan (*Response)) {
	//TODO: write to database
	for res := range in {
		if len(res.Query.Results.DailyTicks) > 0 {
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

	// generate request URLs to concurrently pull down historical prices from yahoo.finance
	in := generateRequestURLs(config.API_URL, config.YQL, config.Symbols, config.YearBeginning, config.YearEnding)

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
	saveDailyQuotes(p)

}


