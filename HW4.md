---
title: "HW4"
author: "Haoran Zhang"
date: "2021/11/18"
output: 
  html_document: 
    keep_md: yes
---




```r
library(parallel)
```


## Problem 1: Make sure your code is nice
Rewrite the following R functions to make them faster. It is OK (and recommended) to take a look at Stackoverflow and Google

```r
temp <- function(n = 100, k = 4, lambda = 4) {
  x <- NULL
  for (i in 1:n)
    x <- rbind(x, rpois(k, lambda))
  return(x)
}
mat<-temp(5,10)
```



```r
# Total row sums
fun1 <- function(mat) {
  n <- nrow(mat)
  ans <- double(n) 
  for (i in 1:n) {
    ans[i] <- sum(mat[i, ])
  }
  ans
}
fun1(mat)
```

```
## [1] 40 45 41 32 39
```




```r
fun1alt <- function(mat) {
  idx<-rowSums(mat)
  idx
}
fun1alt(mat)
```

```
## [1] 40 45 41 32 39
```


```r
# Cumulative sum by row
fun2 <- function(mat) {
  n <- nrow(mat)
  k <- ncol(mat)
  ans <- mat
  for (i in 1:n) {
    for (j in 2:k) {
      ans[i,j] <- mat[i, j] + ans[i, j - 1]
    }
  }
  ans
}
fun2(mat)
```

```
##      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
## [1,]    6    9   14   18   20   28   30   33   37    40
## [2,]    6    9   13   18   21   27   31   38   40    45
## [3,]    4    7   15   21   24   27   31   36   39    41
## [4,]    3    9   13   17   19   23   25   29   30    32
## [5,]    7   10   11   13   18   21   26   30   34    39
```


```r
fun2alt <- function(mat) {
  ans<-t(apply(mat, 1, cumsum))
  ans
}
fun2alt(mat)
```

```
##      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
## [1,]    6    9   14   18   20   28   30   33   37    40
## [2,]    6    9   13   18   21   27   31   38   40    45
## [3,]    4    7   15   21   24   27   31   36   39    41
## [4,]    3    9   13   17   19   23   25   29   30    32
## [5,]    7   10   11   13   18   21   26   30   34    39
```


```r
# Use the data with this code
set.seed(2315)
dat <- matrix(rnorm(200 * 100), nrow = 200)

# Test for the first
microbenchmark::microbenchmark(
  fun1(dat),
  fun1alt(dat), unit = "relative", check = "equivalent"
)
```

```
## Unit: relative
##          expr      min       lq     mean   median       uq       max neval
##     fun1(dat) 6.976019 7.233456 5.461982 8.683176 7.987296 0.7162391   100
##  fun1alt(dat) 1.000000 1.000000 1.000000 1.000000 1.000000 1.0000000   100
```

```r
# Test for the second
microbenchmark::microbenchmark(
  fun2(dat),
  fun2alt(dat), unit = "relative", check = "equivalent"
)
```

```
## Unit: relative
##          expr      min       lq     mean   median       uq       max neval
##     fun2(dat) 4.334792 4.085478 3.500237 3.826045 3.394866 0.9712557   100
##  fun2alt(dat) 1.000000 1.000000 1.000000 1.000000 1.000000 1.0000000   100
```
## Problem 2: Make things run faster with parallel computing
The following function allows simulating PI

```r
sim_pi <- function(n = 1000, i = NULL) {
  p <- matrix(runif(n*2), ncol = 2)
  mean(rowSums(p^2) < 1) * 4
}
# Here is an example of the run
set.seed(156)
sim_pi(1000) # 3.132
```

```
## [1] 3.132
```
In order to get accurate estimates, we can run this function multiple times, with the following code:

```r
# This runs the simulation a 4,000 times, each with 10,000 points
set.seed(1231)
system.time({
  ans <- unlist(lapply(1:4000, sim_pi, n = 10000))
  print(mean(ans))
})
```

```
## [1] 3.14124
```

```
##    user  system elapsed 
##    2.59    0.00    2.60
```
Rewrite the previous code using parLapply() to make it run faster. Make sure you set the seed using clusterSetRNGStream():

```r
cl <- makePSOCKcluster(4)
clusterSetRNGStream(cl, 123)
clusterExport(cl,varlist = c("sim_pi"), envir = environment())

system.time({
  ans <- unlist(parLapply(cl, 1:4000, sim_pi, n = 10000))
  print(mean(ans))
  stopCluster(cl)
})
```

```
## [1] 3.141482
```

```
##    user  system elapsed 
##    0.00    0.00    0.92
```



```r
# YOUR CODE HERE
system.time({
  # YOUR CODE HERE
  ans <- # YOUR CODE HERE
  print(mean(ans))
  # YOUR CODE HERE
})
```

```
## [1] 3.141482
```

```
##    user  system elapsed 
##       0       0       0
```

## SQL
Setup a temporary database by running the following chunk

```r
# install.packages(c("RSQLite", "DBI"))

library(RSQLite)
```

```
## Warning: package 'RSQLite' was built under R version 4.0.3
```

```r
library(DBI)
```

```
## Warning: package 'DBI' was built under R version 4.0.3
```

```r
# Initialize a temporary in memory database
con <- dbConnect(SQLite(), ":memory:")

# Download tables
film <- read.csv("https://raw.githubusercontent.com/ivanceras/sakila/master/csv-sakila-db/film.csv")
film_category <- read.csv("https://raw.githubusercontent.com/ivanceras/sakila/master/csv-sakila-db/film_category.csv")
category <- read.csv("https://raw.githubusercontent.com/ivanceras/sakila/master/csv-sakila-db/category.csv")

# Copy data.frames to database
dbWriteTable(con, "film", film)
dbWriteTable(con, "film_category", film_category)
dbWriteTable(con, "category", category)
```

## Question 1
How many many movies is there avaliable in each rating catagory.

```sql
select rating, count(film_id) as count 
from film 
group by rating
```


<div class="knitsql-table">


Table: 5 records

|rating | count|
|:------|-----:|
|G      |   180|
|NC-17  |   210|
|PG     |   194|
|PG-13  |   223|
|R      |   195|

</div>

## Question 2
What is the average replacement cost and rental rate for each rating category.

```sql
select rating, avg(replacement_cost), avg(rental_rate)
from film
group by rating
```


<div class="knitsql-table">


Table: 5 records

|rating | avg(replacement_cost)| avg(rental_rate)|
|:------|---------------------:|----------------:|
|G      |              20.12333|         2.912222|
|NC-17  |              20.13762|         2.970952|
|PG     |              18.95907|         3.051856|
|PG-13  |              20.40256|         3.034843|
|R      |              20.23103|         2.938718|

</div>

## Question 3
Use table film_category together with film to find the how many films there are witth each category ID

```sql
select category_id, count(film_category.film_id) as count_film_id
from film inner join film_category on film.film_id=film_category.film_id
group by category_id
```


<div class="knitsql-table">


Table: Displaying records 1 - 10

|category_id | count_film_id|
|:-----------|-------------:|
|1           |            64|
|2           |            66|
|3           |            60|
|4           |            57|
|5           |            58|
|6           |            68|
|7           |            62|
|8           |            69|
|9           |            73|
|10          |            61|

</div>
Create a table 

```r
Q3<-dbGetQuery(con, "select category_id, count(film_category.film_id) as count_film_id
from film inner join film_category on film.film_id=film_category.film_id
group by category_id")
dbWriteTable(con, "Q3", Q3)
```

## Question 4
Incorporate table category into the answer to the previous question to find the name of the most popular category.

```sql
select Q3.category_id, category.name, max(count_film_id) as count
from Q3 inner join category on Q3.category_id=category.category_id
```


<div class="knitsql-table">


Table: 1 records

| category_id|name   | count|
|-----------:|:------|-----:|
|          15|Sports |    74|

</div>
clean up

```r
dbDisconnect(con)
```

