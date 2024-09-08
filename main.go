package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type (
	Book struct {
		title       string `json:"title"`
		description string `json:"description"`
		publishDate string `json:"publish_date"`
	}

	Author struct {
		ID        int    `json:"id"`
		Name      string `json:"name"`
		Bio       string `json:"bio"`
		BirthDate string `json:"birth_date"`
		Books     Book   `json:"books"`
	}
)

// Worker function to index product into Elasticsearch
func worker(id int, jobs <-chan Author, es *elasticsearch.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for author := range jobs {
		// Convert the product struct to JSON
		productJSON, err := json.Marshal(author)
		if err != nil {
			log.Fatalf("Error marshaling product to JSON: %s", err)
		}

		// Index the document into Elasticsearch
		req := esapi.IndexRequest{
			Index:      os.Getenv("ELASTICSEARCH_INDEX"), // The index name in Elasticsearch
			DocumentID: fmt.Sprintf("%d", author.ID),     // Use MySQL ID as document ID
			Body:       strings.NewReader(string(productJSON)),
			Refresh:    "true",
		}

		res, err := req.Do(context.Background(), es)
		if err != nil {
			log.Fatalf("Error indexing document: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			log.Printf("Worker %d: Error response from Elasticsearch for document ID %d: %s", id, author.ID, res.String())
		} else {
			log.Printf("Worker %d: Document ID %d indexed successfully", id, author.ID)
		}
	}
}

func main() {
	err := godotenv.Load("./.env")
	if err != nil {
		log.Println(err.Error())
	}

	// Connect to MySQL
	dsn := os.Getenv("MYSQL_DSN")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Error opening MySQL connection:", err)
	}
	defer db.Close()

	// Elasticsearch API Key (replace with your actual key)
	apiKey := os.Getenv("ELASTICSEARCH_API_KEY")

	// Connect to Elasticsearch with API key
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			os.Getenv("ELASTICSEARCH_URL"),
		},
		APIKey: apiKey, // Use APIKey field to authenticate
	})
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// Query MySQL to fetch data
	rows, err := db.Query(
		`SELECT authors.id, authors.name, authors.bio, authors.birth_date,
		books.title AS title, books.description AS description, books.publish_date AS publish_date 
			FROM authors 
			LEFT JOIN books ON authors.id = books.author_id 
			WHERE books.title IS NOT NULL
	`)
	if err != nil {
		log.Fatal("Error executing MySQL query:", err)
	}
	defer rows.Close()

	// Create a worker pool
	numWorkers := 5
	jobs := make(chan Author, numWorkers)
	var wg sync.WaitGroup

	// Start worker pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, es, &wg)
	}

	// Fetch rows from MySQL and send them to workers via the jobs channel
	for rows.Next() {
		var authors Author

		if &authors.Books.title == nil || &authors.Books.description == nil || &authors.Books.publishDate == nil {
			authors.Books = Book{}
		} else {
			if err := rows.Scan(
				&authors.ID,
				&authors.Name,
				&authors.Bio,
				&authors.BirthDate,
				&authors.Books.title,
				&authors.Books.description,
				&authors.Books.publishDate,
			); err != nil {
				log.Fatal("Error scanning row:", err)
			}
		}

		// Send product data to the jobs channel
		jobs <- authors
	}

	// Close the jobs channel once all rows are processed
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("All authors migrated to Elasticsearch")
}
