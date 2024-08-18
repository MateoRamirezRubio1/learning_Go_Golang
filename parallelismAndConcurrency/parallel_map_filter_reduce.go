package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// generateRandomSlice generates a slice of random integers.
func generateRandomSlice(size, min, max int) []int {
	// Seed the random number generator with the current time.
	rand.Seed(time.Now().UnixNano())
	// Create a slice of the specified size.
	slice := make([]int, size)
	// Populate the slice with random integers between min and max.
	for i := range slice {
		slice[i] = rand.Intn(max-min+1) + min
	}
	return slice
}

// parallelMap applies a mapping function to each element of a slice in parallel.
func parallelMap(nums []int, mapFunc func(int) int, numWorkers int) []int {
	n := len(nums)
	// Create a result slice of the same length as the input slice.
	result := make([]int, n)
	// Calculate the chunk size for each worker.
	chunkSize := n / numWorkers
	// Create a WaitGroup to synchronize the workers.
	var wg sync.WaitGroup

	// Start the workers.
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = n
		}
		wg.Add(1)
		// Each worker applies the mapping function to its chunk of the slice.
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				result[j] = mapFunc(nums[j])
			}
		}(start, end)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return result
}

// parallelFilter filters a slice in parallel using a filtering function.
func parallelFilter(nums []int, filterFunc func(int) bool, numWorkers int) []int {
	n := len(nums)
	chunkSize := n / numWorkers
	var wg sync.WaitGroup
	// Create a Mutex to protect the result slice.
	var mu sync.Mutex
	result := make([]int, 0, n)

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = n
		}
		wg.Add(1)
		// Each worker filters its chunk of the slice and appends the results to the local result slice.
		go func(start, end int) {
			defer wg.Done()
			localResult := make([]int, 0, end-start)
			for j := start; j < end; j++ {
				if filterFunc(nums[j]) {
					localResult = append(localResult, nums[j])
				}
			}
			// Lock the Mutex and append the local result slice to the global result slice.
			mu.Lock()
			result = append(result, localResult...)
			mu.Unlock()
		}(start, end)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return result
}

// parallelReduce reduces a slice in parallel using a reduction function.
func parallelReduce(nums []int, reduceFunc func(int, int) int, numWorkers int) int {
	n := len(nums)
	chunkSize := n / numWorkers
	var wg sync.WaitGroup
	var mu sync.Mutex
	var result int32

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = n
		}
		wg.Add(1)
		// Each worker reduces its chunk of the slice and atomically adds the result to the global result variable.
		go func(start, end int) {
			defer wg.Done()
			localResult := nums[start]
			for j := start + 1; j < end; j++ {
				localResult = reduceFunc(localResult, nums[j])
			}
			// Lock the Mutex and atomically add the local result to the global result variable.
			mu.Lock()
			atomic.AddInt32(&result, int32(localResult))
			mu.Unlock()
		}(start, end)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return int(result)
}

func main() {
	// Generate a random slice of 1000000 integers between 1 and 9.
	nums := generateRandomSlice(1000000, 1, 9)
	fmt.Println("First numbers:", nums[:10])

	// Start measuring execution time.
	start := time.Now()
	// Get the number of CPUs available.
	numWorkers := runtime.NumCPU()

	// Apply the mapping function in parallel.
	mapped := parallelMap(nums, func(x int) int { return x * 2 }, numWorkers)
	// Apply the filtering function in parallel.
	filtered := parallelFilter(mapped, func(x int) bool { return x > 10 }, numWorkers)
	// Apply the reduction function in parallel.
	result := parallelReduce(filtered, func(acc, x int) int { return acc + x }, numWorkers)

	fmt.Println("Result:", result)
	// Stop measuring execution time and print the duration.
	duration := time.Since(start)
	fmt.Println("Execution time:", duration)
}
