// Command fred2ch loads a single Fred II series to a ClickHouse table.
// Call:
//
// Required command line arguments:
//    -series  Fred II series id
//    -table   destination ClickHouse table.
//    -api     Fred II API key
//
// Optional command line arguments:
//    -host           IP of ClickHouse database. Default: 127.0.0.1
//    -user           ClickHouse user. Default: "default"
//    -password       ClickHouse password. Default: ""
//
// The table created has these fields:
//
// seriesId    String     series ID requested
// month       Date       date of date
// value       Float32    value of metric
//
// All months available for the series are loaded.
// Series names are case insensitive.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	"io"
	"log"
	"net/http"
	"time"
)

// Datum is the data for a single month
type Datum struct {
	RtStart string `json:"realtime_start,omitempty"`
	RtEnd   string `json:"realtime_end,omitempty"`
	Month   string `json:"date,omitempty"`
	Value   string `json:"value,omitempty"`
}

// Series is the outermost struct returned by the http Get
type Series struct {
	ObservationStart string  `json:"observation_start"`
	ObservationEnd   string  `json:"observation_end"`
	Units            string  `json:"units"`
	OrderBy          string  `json:"order_by"`
	Count            int     `json:"count"`
	RealtimeStart    string  `json:"realtime_start"`
	RealtimeEnd      string  `json:"realtime_end"`
	OutputType       int     `json:"output_type"`
	FileType         string  `json:"file_type"`
	SortOrder        string  `json:"sort_order"`
	Offset           int     `json:"offset"`
	Limit            int     `json:"limit"`
	Results          []Datum `json:"observations"`
}

// apiUrl is the address of the API
const apiUrl = "https://api.stlouisfed.org/fred/series/observations"

func main() {

	hostPtr := flag.String("host", "127.0.0.1", "string")
	userPtr := flag.String("user", "", "string")
	passwordPtr := flag.String("password", "", "string")

	apiKeyPtr := flag.String("api", "", "string")
	seriesPtr := flag.String("series", "", "string")

	tablePtr := flag.String("table", "", "string")

	// fields we can subset/segment on
	flag.Parse()

	con, err := chutils.NewConnect(*hostPtr, *userPtr, *passwordPtr, clickhouse.Settings{"max_memory_usage": 40000000000})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if e := con.Close(); e != nil {
			fmt.Println(e)
		}
	}()

	results, e := getSeries(*seriesPtr, *apiKeyPtr)
	if e != nil {
		log.Fatalln(e)
	}

	if e := loadSeries(results, *seriesPtr, *tablePtr, con); e != nil {
		log.Fatalln(e)
	}

}

// getSeries pulls the data for the series seriesId.
func getSeries(seriesId string, apiKey string) (*Series, error) {
	// Build url for Get
	source := fmt.Sprintf("%s?series_id=%s&api_key=%s&file_type=json", apiUrl, seriesId, apiKey)
	resp, e := http.Get(source)
	if e != nil {
		return nil, e
	}
	body, e := io.ReadAll(resp.Body)
	if e := resp.Body.Close(); e != nil {
		return nil, e
	}
	if e != nil {
		return nil, e
	}

	var parsed Series
	if e = json.Unmarshal(body, &parsed); e != nil {
		return nil, e
	}
	if parsed.Results == nil {
		return nil, fmt.Errorf("no data returned for series %s", seriesId)
	}
	return &parsed, nil
}

// maketable creates the output table.  If there's an existing table, it's dropped.
func makeTable(seriesId string, table string, con *chutils.Connect) error {
	// build field defs
	fds := make(map[int]*chutils.FieldDef)
	fd := &chutils.FieldDef{Name: "seriesId",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Legal:       &chutils.LegalValues{},
		Description: "Fred II series id"}
	fds[0] = fd
	fd = &chutils.FieldDef{Name: "month",
		ChSpec:      chutils.ChField{Base: chutils.ChDate},
		Legal:       &chutils.LegalValues{},
		Description: "month"}
	fds[1] = fd
	fd = &chutils.FieldDef{Name: "value",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Legal:       &chutils.LegalValues{},
		Description: fmt.Sprintf("metric value for series %s", seriesId)}
	fds[2] = fd

	td := chutils.NewTableDef("month", chutils.MergeTree, fds)
	// check everything is OK with our TableDef
	if e := td.Check(); e != nil {
		return e
	}
	// Create table
	if e := td.Create(con, table); e != nil {
		return e
	}
	return nil
}

// loadSeries pushes the returned series to ClickHouse.  Any existing version of table is dropped.
func loadSeries(data *Series, seriesId string, table string, con *chutils.Connect) error {
	// missing value for month if month is not valid
	var missing = time.Date(1901, 1, 1, 0, 0, 0, 0, time.UTC)
	if e := makeTable(seriesId, table, con); e != nil {
		return e
	}
	// Create a writer
	wtr := s.NewWriter(table, con)
	defer func() {
		if e := wtr.Close(); e != nil {
			fmt.Println(e)
		}
	}()
	loaded := 0
	// work through the array
	for _, d := range data.Results {
		// check date is legit
		dt, e := time.Parse("2006-01-02", d.Month)
		if e != nil {
			dt = missing
		}
		// each row just has 3 values: seriesId, month, value
		line := fmt.Sprintf("'%s','%s',%v", seriesId, dt.Format("2006-01-02"), d.Value)
		if _, e := wtr.Write([]byte(line)); e != nil {
			return e
		}
		loaded++
	}
	if e := wtr.Insert(); e != nil {
		return e
	}
	fmt.Printf("%d rows for series %s loaded to table %s\n", loaded, seriesId, table)
	return nil
}
