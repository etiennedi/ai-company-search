package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	goswagger "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
)

type company struct {
	Name             string
	Symbol           string
	Location         string
	Founded          *time.Time
	Sector           string
	SubIndustry      string
	YearHigh         float32
	YearLow          float32
	DividendYield    float32
	EBITDA           float64
	EarningsPerShare float32
	MarketCap        float64
	Price            float32
	PricePerBook     float32
	PricePerEarnings float32
	PricePerSales    float32
}

func main() {
	companies := parseCompanies()
	companies = extendWithFinancialData(companies)
	client := weaviateClient()
	importSchema(client)
	importCompanies(client, companies)
}

func importSchema(client *client.WeaviateDecentralisedKnowledgeGraph) {
	tc := &models.Class{
		Class: "Company",
		Properties: []*models.Property{
			&models.Property{
				Name:     "symbol",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "name",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "sector",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "subIndustry",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "location",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "locationCoordinates",
				DataType: []string{"geoCoordinates"},
			},
			&models.Property{
				Name:     "yearHigh",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "yearLow",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "dividendYield",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "ebitda",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "earningsPerShare",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "marketCap",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "price",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "pricePerBook",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "pricePerEarnings",
				DataType: []string{"number"},
			},
			&models.Property{
				Name:     "pricePerSales",
				DataType: []string{"number"},
			},
		},
	}

	params := schema.NewSchemaThingsCreateParams().WithThingClass(tc)
	_, err := client.Schema.SchemaThingsCreate(params, nil)
	fatal(err)
}

func importCompanies(client *client.WeaviateDecentralisedKnowledgeGraph,
	companies []company) {
	for i, c := range companies {

		thing := models.Thing{
			Class: "Company",
			Schema: map[string]interface{}{
				"symbol":           c.Symbol,
				"name":             c.Name,
				"sector":           c.Sector,
				"subIndustry":      c.SubIndustry,
				"location":         c.Location,
				"dividendYield":    c.DividendYield,
				"ebitda":           c.EBITDA,
				"earningsPerShare": c.EarningsPerShare,
				"marketCap":        c.MarketCap,
				"price":            c.Price,
				"pricePerBook":     c.PricePerBook,
				"pricePerEarnings": c.PricePerEarnings,
				"pricePerSales":    c.PricePerSales,
			},
		}

		l := lookupCoordinates(c.Location)
		if l != nil {
			thing.Schema.(map[string]interface{})["locationCoordinates"] = l
		}

		params := things.NewThingsCreateParams().WithBody(&thing)
		_, err := client.Things.ThingsCreate(params, nil)
		fatal(err)

		fmt.Print(".")
		if i != 0 && i%50 == 0 {
			fmt.Print("\n")
		}
	}
}

func parseCompanies() []company {
	file, err := os.Open("./list.txt")
	fatal(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var companies []company

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "\t")
		var founded *time.Time
		if len(parts) >= 9 {
			// silently ignore ill-formatted
			t, _ := time.Parse("2006", parts[8])
			founded = &t
		}
		companies = append(companies, company{
			Symbol:      parts[0],
			Name:        parts[1],
			Sector:      parts[3],
			SubIndustry: parts[4],
			Location:    parts[5],
			Founded:     founded,
		})
	}

	return companies
}

func fatal(err error) {
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func weaviateClient() *client.WeaviateDecentralisedKnowledgeGraph {
	transport := goswagger.New("localhost:8080", "/v1", []string{"http"})
	client := client.New(transport, strfmt.Default)
	return client
}

var locations = map[string]*models.GeoCoordinates{
	"San Jose, California":      {Latitude: 37.334789, Longitude: 121.888138},
	"San Francisco, California": {Latitude: 37.774929, Longitude: -122.419418},
	"Chicago, Illinois":         {Latitude: 41.878113, Longitude: -87.629799},
	"Atlanta, Georgia":          {Latitude: 33.748997, Longitude: -84.387985},
	"Houston, Texas":            {Latitude: 29.760427, Longitude: -95.369804},
	"New York, New York":        {Latitude: 40.712776, Longitude: -74.005974},
}

func lookupCoordinates(location string) *models.GeoCoordinates {
	return locations[location]
}

func extendWithFinancialData(companies []company) []company {
	financialLookup := buildFinancialLookup()

	for i, company := range companies {
		financial, ok := financialLookup[company.Symbol]
		if !ok {
			continue
		}

		companies[i].YearHigh = financial.YearHigh
		companies[i].YearLow = financial.YearLow
		companies[i].DividendYield = financial.DividendYield
		companies[i].EBITDA = financial.EBITDA
		companies[i].EarningsPerShare = financial.EarningsPerShare
		companies[i].MarketCap = financial.MarketCap
		companies[i].Price = financial.Price
		companies[i].PricePerBook = financial.PricePerBook
		companies[i].PricePerEarnings = financial.PricePerEarnings
		companies[i].PricePerSales = financial.PricePerSales
		companies[i].Symbol = financial.Symbol
	}

	return companies
}

type financeData struct {
	YearHigh         float32 `json:"52 Week High"`
	YearLow          float32 `json:"52 Week Low"`
	DividendYield    float32 `json:"Dividend Yield"`
	EBITDA           float64 `json:"EBITDA"`
	EarningsPerShare float32 `json:"Earnings/Share"`
	MarketCap        float64 `json:"Market Cap"`
	Price            float32 `json:"Price"`
	PricePerBook     float32 `json:"Price/Book"`
	PricePerEarnings float32 `json:"Price/Earnings"`
	PricePerSales    float32 `json:"Price/Sales"`
	Symbol           string  `json:"Symbol"`
}

func buildFinancialLookup() map[string]financeData {
	raw, err := ioutil.ReadFile("./financial.json")
	fatal(err)

	var list []financeData

	err = json.Unmarshal(raw, &list)
	fatal(err)

	lookup := map[string]financeData{}
	for _, item := range list {
		lookup[item.Symbol] = item
	}

	return lookup
}
