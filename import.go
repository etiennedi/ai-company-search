package main

import (
	"bufio"
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
	Name        string
	Symbol      string
	Location    string
	Founded     *time.Time
	Sector      string
	SubIndustry string
}

func main() {
	companies := parseCompanies()
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
		},
	}

	params := schema.NewWeaviateSchemaThingsCreateParams().WithThingClass(tc)
	_, err := client.Schema.WeaviateSchemaThingsCreate(params, nil)
	fatal(err)
}

func importCompanies(client *client.WeaviateDecentralisedKnowledgeGraph,
	companies []company) {
	for _, c := range companies {
		thing := models.Thing{
			Class: "Company",
			Schema: map[string]interface{}{
				"symbol":      c.Symbol,
				"name":        c.Name,
				"sector":      c.Sector,
				"subIndustry": c.SubIndustry,
				"location":    c.Location,
			},
		}
		params := things.NewWeaviateThingsCreateParams().WithBody(&thing)
		_, err := client.Things.WeaviateThingsCreate(params, nil)
		fatal(err)
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
	transport := goswagger.New("localhost:8080", "/weaviate/v1", []string{"http"})
	client := client.New(transport, strfmt.Default)
	return client
}
