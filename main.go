package main

import "encoding/json"
import "io/ioutil"
import "fmt"
import "net/http"
import "net/url"
import "strings"

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

func generateRequestURLs(apiUrl string, yql string, symbols []Symbol, yearBegin int, yearEnd int) string {
	for i := 0; i < len(symbols); i++ {
		for year := yearEnd; year >= yearBegin; year-- {
			//TODO: output URLs to a channel
			//fmt.Printf("%s\n", formatRequestURL(apiUrl, yql, symbols[i].Sym, year))
		} 
	}

	return ""
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

func getResponseBody(requestURL string) []byte {

	//TODO: don't like error handling, is there a better way?
	resp, err := http.Get(requestURL)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	return body
}

func parseResponse(responseBody []byte) (* Response) {
	res := Response{}
	if err := json.Unmarshal(responseBody, &res); err != nil {
		return nil
	}
	return &res
}

func saveDailyQuotes(response *Response) {
	//TODO: write to database
	fmt.Printf("Total daily ticks for %s : %d\n", response.Query.Results.DailyTicks[0].Symbol, len(response.Query.Results.DailyTicks))
}

func main() {

	// load configuration
	config := loadConfig()

	// generate request URLs to concurrently pull down historical prices from yahoo.finance
	generateRequestURLs(config.API_URL, config.YQL, config.Symbols, config.YearBeginning, config.YearEnding)

	//TODO: use channel pipeline to process multiple requests 
	requestUrl := formatRequestURL(config.API_URL, config.YQL, config.Symbols[0].Sym, config.YearEnding)
	body := getResponseBody(requestUrl)
	response := parseResponse(body)
	saveDailyQuotes(response)

}

