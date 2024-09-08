# golang_elastic

This repository used for import data from MySQL to Elasticsearch

This is just the simple purpose, we can use https://github.com/elastic/go-elasticsearch for more better one

## MySQL Data : 
Table `books` :
![books_table](https://github.com/user-attachments/assets/ed7b66ff-bc3d-4e6f-be4b-acc88f76953c)

Table `authors`
![author_table](https://github.com/user-attachments/assets/508d1531-9d34-4b1a-9486-afa3d42dd370)

## Goroutine implementation for better result
I also implement `Goroutine` for better result. A goroutine is a lightweight thread managed by the Go runtime. go f(x, y, z). starts a new goroutine running

```
// Create a worker pool
	numWorkers := 5
	jobs := make(chan Author, numWorkers)
	var wg sync.WaitGroup

	// Start worker pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, es, &wg)
	}
```
