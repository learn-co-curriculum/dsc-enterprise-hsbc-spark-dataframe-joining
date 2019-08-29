
# Joining

* It is often times required to join `DataTable` of to gain insight
* Much like a database, Spark has varying joins
* In Spark there are row joins called `union` and column joins like `left join`
* There are varying joins: `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`, `right_outer`, `left_semi`, `left_anti`.

## Joining Rows

Given two `DataSet`s or `DataFrame`s, we can join the rows by using `union`


```scala
val countriesMedalCountDF =
      Seq(("United States", "100m Freestyle", 1, 0, 3),
        ("Spain", "100m Butterfly", 2, 1, 1),
        ("Japan", "100m Butterfly", 0, 3, 0),
        ("Spain", "100m Freestyle", 0, 0, 3),
        ("Uruguay", "100m Breaststroke", 0, 1, 0),
        ("United States", "100m Breaststroke", 2, 2, 0))
        .toDF("Country", "Event", "Gold", "Silver", "Bronze")

val countriesMedalCountDF2 =
      Seq(("United States", "100m Freestyle", 1, 0, 3),
        ("Spain", "100m Backstroke", 2, 1, 1),
        ("Spain", "200m Breaststroke", 1, 0, 0),
        ("Spain", "500m Freestyle", 3, 0, 0),
        ("Spain", "1000m Freestyle", 2, 1, 0),
        ("United States", "100m Breaststroke", 2, 2, 0))
        .toDF("Country", "Event", "Gold", "Silver", "Bronze")
```


    Intitializing Scala interpreter ...



    Spark Web UI available at http://fb62c0c5dfbd:4042
    SparkContext available as 'sc' (version = 2.4.3, master = local[*], app id = local-1563898268663)
    SparkSession available as 'spark'
    





    countriesMedalCountDF: org.apache.spark.sql.DataFrame = [Country: string, Event: string ... 3 more fields]
    countriesMedalCountDF2: org.apache.spark.sql.DataFrame = [Country: string, Event: string ... 3 more fields]
    



### A `union` joins all the rows


```scala
val unionDF = countriesMedalCountDF.union(countriesMedalCountDF2)
unionDF.show(30)
```

    +-------------+-----------------+----+------+------+
    |      Country|            Event|Gold|Silver|Bronze|
    +-------------+-----------------+----+------+------+
    |United States|   100m Freestyle|   1|     0|     3|
    |        Spain|   100m Butterfly|   2|     1|     1|
    |        Japan|   100m Butterfly|   0|     3|     0|
    |        Spain|   100m Freestyle|   0|     0|     3|
    |      Uruguay|100m Breaststroke|   0|     1|     0|
    |United States|100m Breaststroke|   2|     2|     0|
    |United States|   100m Freestyle|   1|     0|     3|
    |        Spain|  100m Backstroke|   2|     1|     1|
    |        Spain|200m Breaststroke|   1|     0|     0|
    |        Spain|   500m Freestyle|   3|     0|     0|
    |        Spain|  1000m Freestyle|   2|     1|     0|
    |United States|100m Breaststroke|   2|     2|     0|
    +-------------+-----------------+----+------+------+
    
    




    unionDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Country: string, Event: string ... 3 more fields]
    



### We can be selective which rows will be joined, by using a `where` or  `filter`


```scala
val unionDF = countriesMedalCountDF.union(countriesMedalCountDF2.where("country = 'Spain'"))
unionDF.show(30)
```

    +-------------+-----------------+----+------+------+
    |      Country|            Event|Gold|Silver|Bronze|
    +-------------+-----------------+----+------+------+
    |United States|   100m Freestyle|   1|     0|     3|
    |        Spain|   100m Butterfly|   2|     1|     1|
    |        Japan|   100m Butterfly|   0|     3|     0|
    |        Spain|   100m Freestyle|   0|     0|     3|
    |      Uruguay|100m Breaststroke|   0|     1|     0|
    |United States|100m Breaststroke|   2|     2|     0|
    |        Spain|  100m Backstroke|   2|     1|     1|
    |        Spain|200m Breaststroke|   1|     0|     0|
    |        Spain|   500m Freestyle|   3|     0|     0|
    |        Spain|  1000m Freestyle|   2|     1|     0|
    +-------------+-----------------+----+------+------+
    
    




    unionDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Country: string, Event: string ... 3 more fields]
    



### Using `union` just add just one `Row` to the `DataFrame`


```scala
val addedItalyDF = countriesMedalCountDF.union(Seq(("Italy", "500m Freestyle", 0, 1, 0)).toDF("Country", "Event", "Gold", "Silver", "Bronze"))
addedItalyDF.show()
```

    +-------------+-----------------+----+------+------+
    |      Country|            Event|Gold|Silver|Bronze|
    +-------------+-----------------+----+------+------+
    |United States|   100m Freestyle|   1|     0|     3|
    |        Spain|   100m Butterfly|   2|     1|     1|
    |        Japan|   100m Butterfly|   0|     3|     0|
    |        Spain|   100m Freestyle|   0|     0|     3|
    |      Uruguay|100m Breaststroke|   0|     1|     0|
    |United States|100m Breaststroke|   2|     2|     0|
    |        Italy|   500m Freestyle|   0|     1|     0|
    +-------------+-----------------+----+------+------+
    
    




    addedItalyDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Country: string, Event: string ... 3 more fields]
    



## Joining Columns

How we join columns, we have a selection of the joins to choose from each with its own side effect, and we will see each of these in turn.


```scala
import org.apache.spark.sql.DataFrame
val cities: DataFrame = Seq(
    (1, "San Francisco", "CA"),
    (2, "Dallas", "TX"),
    (3, "Pittsburgh", "PA"),
    (4, "Buffalo", "NY"),
    (5, "Oklahoma City", "OK"),
    (6, "New York", "NY"),
    (7, "Los Angeles", "CA"),
    (8, "Omaha", "NE")).toDF("id", "city", "state")

val teams: DataFrame = Seq(
    (1, "Los Angeles", "Rams", "Football"),
    (2, "Los Angeles", "Dodgers", "Baseball"),
    (3, "New York", "Giants", "Football"),
    (4, "San Francisco", "Giants", "Baseball"),
    (5, "Buffalo", "Bills", "Football"),
    (6, "Pittsburg", "Pirates", "Baseball"),
    (7, "San Francisco", "49ers", "Football"),
    (8, "San Diego", "Padres", "Baseball"),
    (9, "Seattle", "Mariners", "Baseball"),
    (10, "Seattle", "Sounders", "Soccer"),
    (11, "Portland", "Timbers", "Soccer"),
    (12, "Pittsburgh", "Steelers", "Football")).toDF("id", "city", "team", "sport_type")
```




    import org.apache.spark.sql.DataFrame
    cities: org.apache.spark.sql.DataFrame = [id: int, city: string ... 1 more field]
    teams: org.apache.spark.sql.DataFrame = [id: int, city: string ... 2 more fields]
    



## Inner Joins

* `join` performs an inner join with a given expression
* Rows from either table that are unmatched in the other table are not returned
* Notice that there is no `Omaha`, `Oklahoma City` or `Dallas` since there isn't a corresponding right value


```scala
val innerjoin = cities
      .join(teams, cities.col("city") === teams.col("city"))
innerjoin.show()
```

    +---+-------------+-----+---+-------------+--------+----------+
    | id|         city|state| id|         city|    team|sport_type|
    +---+-------------+-----+---+-------------+--------+----------+
    |  7|  Los Angeles|   CA|  1|  Los Angeles|    Rams|  Football|
    |  7|  Los Angeles|   CA|  2|  Los Angeles| Dodgers|  Baseball|
    |  6|     New York|   NY|  3|     New York|  Giants|  Football|
    |  1|San Francisco|   CA|  4|San Francisco|  Giants|  Baseball|
    |  4|      Buffalo|   NY|  5|      Buffalo|   Bills|  Football|
    |  1|San Francisco|   CA|  7|San Francisco|   49ers|  Football|
    |  3|   Pittsburgh|   PA| 12|   Pittsburgh|Steelers|  Football|
    +---+-------------+-----+---+-------------+--------+----------+
    
    




    innerjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    



## Explicit Inner Joins

* We can explicitly set an `inner` join by declaring it as a joinType
* Below we set `joinType`, you can leave the assignment `joinType` if you like


```scala
val innerjoin = cities
      .join(teams, cities.col("city") === teams.col("city"), joinType="inner")
innerjoin.show()
```

    +---+-------------+-----+---+-------------+--------+----------+
    | id|         city|state| id|         city|    team|sport_type|
    +---+-------------+-----+---+-------------+--------+----------+
    |  7|  Los Angeles|   CA|  1|  Los Angeles|    Rams|  Football|
    |  7|  Los Angeles|   CA|  2|  Los Angeles| Dodgers|  Baseball|
    |  6|     New York|   NY|  3|     New York|  Giants|  Football|
    |  1|San Francisco|   CA|  4|San Francisco|  Giants|  Baseball|
    |  4|      Buffalo|   NY|  5|      Buffalo|   Bills|  Football|
    |  1|San Francisco|   CA|  7|San Francisco|   49ers|  Football|
    |  3|   Pittsburgh|   PA| 12|   Pittsburgh|Steelers|  Football|
    +---+-------------+-----+---+-------------+--------+----------+
    
    




    innerjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    



## Left Joins

* `joinType` of `left` or `left_outer` performs an left join with a given expression
* Returns all records from the left `DataFrame`, and the matched records from the right `DataFrame`
* The result is `null` from the right side, if there is no match
* Notice that there are `null`s for `Omaha`, `Oklahoma City` or `Dallas`


```scala
val leftjoin = cities
      .join(teams, cities.col("city") === teams.col("city"), joinType="left")
leftjoin.show()
```

    +---+-------------+-----+----+-------------+--------+----------+
    | id|         city|state|  id|         city|    team|sport_type|
    +---+-------------+-----+----+-------------+--------+----------+
    |  1|San Francisco|   CA|   7|San Francisco|   49ers|  Football|
    |  1|San Francisco|   CA|   4|San Francisco|  Giants|  Baseball|
    |  2|       Dallas|   TX|null|         null|    null|      null|
    |  3|   Pittsburgh|   PA|  12|   Pittsburgh|Steelers|  Football|
    |  4|      Buffalo|   NY|   5|      Buffalo|   Bills|  Football|
    |  5|Oklahoma City|   OK|null|         null|    null|      null|
    |  6|     New York|   NY|   3|     New York|  Giants|  Football|
    |  7|  Los Angeles|   CA|   2|  Los Angeles| Dodgers|  Baseball|
    |  7|  Los Angeles|   CA|   1|  Los Angeles|    Rams|  Football|
    |  8|        Omaha|   NE|null|         null|    null|      null|
    +---+-------------+-----+----+-------------+--------+----------+
    
    




    leftjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    




```scala
val outerleftjoin = cities
      .join(teams, cities.col("city") === teams.col("city"), joinType="left_outer")
outerleftjoin.show()
```

    +---+-------------+-----+----+-------------+--------+----------+
    | id|         city|state|  id|         city|    team|sport_type|
    +---+-------------+-----+----+-------------+--------+----------+
    |  1|San Francisco|   CA|   7|San Francisco|   49ers|  Football|
    |  1|San Francisco|   CA|   4|San Francisco|  Giants|  Baseball|
    |  2|       Dallas|   TX|null|         null|    null|      null|
    |  3|   Pittsburgh|   PA|  12|   Pittsburgh|Steelers|  Football|
    |  4|      Buffalo|   NY|   5|      Buffalo|   Bills|  Football|
    |  5|Oklahoma City|   OK|null|         null|    null|      null|
    |  6|     New York|   NY|   3|     New York|  Giants|  Football|
    |  7|  Los Angeles|   CA|   2|  Los Angeles| Dodgers|  Baseball|
    |  7|  Los Angeles|   CA|   1|  Los Angeles|    Rams|  Football|
    |  8|        Omaha|   NE|null|         null|    null|      null|
    +---+-------------+-----+----+-------------+--------+----------+
    
    




    outerleftjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    



## Right Joins

* `right` or `right_outer` performs an right join with a given expression
* Returns all records from the right `DataFrame`, and the matched records from the left `DataFame`. 
* The result is `null` from the left side, when there is no match.


```scala
val rightjoin = cities
      .join(teams, cities.col("city") === teams.col("city"), joinType="right")
rightjoin.show()
```

    +----+-------------+-----+---+-------------+--------+----------+
    |  id|         city|state| id|         city|    team|sport_type|
    +----+-------------+-----+---+-------------+--------+----------+
    |   7|  Los Angeles|   CA|  1|  Los Angeles|    Rams|  Football|
    |   7|  Los Angeles|   CA|  2|  Los Angeles| Dodgers|  Baseball|
    |   6|     New York|   NY|  3|     New York|  Giants|  Football|
    |   1|San Francisco|   CA|  4|San Francisco|  Giants|  Baseball|
    |   4|      Buffalo|   NY|  5|      Buffalo|   Bills|  Football|
    |null|         null| null|  6|    Pittsburg| Pirates|  Baseball|
    |   1|San Francisco|   CA|  7|San Francisco|   49ers|  Football|
    |null|         null| null|  8|    San Diego|  Padres|  Baseball|
    |null|         null| null|  9|      Seattle|Mariners|  Baseball|
    |null|         null| null| 10|      Seattle|Sounders|    Soccer|
    |null|         null| null| 11|     Portland| Timbers|    Soccer|
    |   3|   Pittsburgh|   PA| 12|   Pittsburgh|Steelers|  Football|
    +----+-------------+-----+---+-------------+--------+----------+
    
    




    rightjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    




```scala
val rightjoin = cities
      .join(teams, cities.col("city") === teams.col("city"), joinType="right_outer")
rightjoin.show()
```

    +----+-------------+-----+---+-------------+--------+----------+
    |  id|         city|state| id|         city|    team|sport_type|
    +----+-------------+-----+---+-------------+--------+----------+
    |   7|  Los Angeles|   CA|  1|  Los Angeles|    Rams|  Football|
    |   7|  Los Angeles|   CA|  2|  Los Angeles| Dodgers|  Baseball|
    |   6|     New York|   NY|  3|     New York|  Giants|  Football|
    |   1|San Francisco|   CA|  4|San Francisco|  Giants|  Baseball|
    |   4|      Buffalo|   NY|  5|      Buffalo|   Bills|  Football|
    |null|         null| null|  6|    Pittsburg| Pirates|  Baseball|
    |   1|San Francisco|   CA|  7|San Francisco|   49ers|  Football|
    |null|         null| null|  8|    San Diego|  Padres|  Baseball|
    |null|         null| null|  9|      Seattle|Mariners|  Baseball|
    |null|         null| null| 10|      Seattle|Sounders|    Soccer|
    |null|         null| null| 11|     Portland| Timbers|    Soccer|
    |   3|   Pittsburgh|   PA| 12|   Pittsburgh|Steelers|  Football|
    +----+-------------+-----+---+-------------+--------+----------+
    
    




    rightjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    



## Outer Joins
* `join` with a quality of `outer` performs an `outer` join with a given expression
* Unmatched rows in one or both `DataFrame`s can be returned
* `outer_join` and `full_outer_join` return the same result.


```scala
val outerjoin = cities.join(teams, cities.col("city") === teams.col("city"), joinType = "outer")
outerjoin.show()
```

    +----+-------------+-----+----+-------------+--------+----------+
    |  id|         city|state|  id|         city|    team|sport_type|
    +----+-------------+-----+----+-------------+--------+----------+
    |null|         null| null|   6|    Pittsburg| Pirates|  Baseball|
    |   8|        Omaha|   NE|null|         null|    null|      null|
    |   2|       Dallas|   TX|null|         null|    null|      null|
    |   7|  Los Angeles|   CA|   1|  Los Angeles|    Rams|  Football|
    |   7|  Los Angeles|   CA|   2|  Los Angeles| Dodgers|  Baseball|
    |   5|Oklahoma City|   OK|null|         null|    null|      null|
    |null|         null| null|   8|    San Diego|  Padres|  Baseball|
    |   1|San Francisco|   CA|   4|San Francisco|  Giants|  Baseball|
    |   1|San Francisco|   CA|   7|San Francisco|   49ers|  Football|
    |null|         null| null|  11|     Portland| Timbers|    Soccer|
    |   3|   Pittsburgh|   PA|  12|   Pittsburgh|Steelers|  Football|
    |null|         null| null|   9|      Seattle|Mariners|  Baseball|
    |null|         null| null|  10|      Seattle|Sounders|    Soccer|
    |   4|      Buffalo|   NY|   5|      Buffalo|   Bills|  Football|
    |   6|     New York|   NY|   3|     New York|  Giants|  Football|
    +----+-------------+-----+----+-------------+--------+----------+
    
    




    outerjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    




```scala
val fullouterjoin = cities.join(teams, cities.col("city") === teams.col("city"), joinType = "full_outer")
fullouterjoin.show()
```

    +----+-------------+-----+----+-------------+--------+----------+
    |  id|         city|state|  id|         city|    team|sport_type|
    +----+-------------+-----+----+-------------+--------+----------+
    |null|         null| null|   6|    Pittsburg| Pirates|  Baseball|
    |   8|        Omaha|   NE|null|         null|    null|      null|
    |   2|       Dallas|   TX|null|         null|    null|      null|
    |   7|  Los Angeles|   CA|   1|  Los Angeles|    Rams|  Football|
    |   7|  Los Angeles|   CA|   2|  Los Angeles| Dodgers|  Baseball|
    |   5|Oklahoma City|   OK|null|         null|    null|      null|
    |null|         null| null|   8|    San Diego|  Padres|  Baseball|
    |   1|San Francisco|   CA|   4|San Francisco|  Giants|  Baseball|
    |   1|San Francisco|   CA|   7|San Francisco|   49ers|  Football|
    |null|         null| null|  11|     Portland| Timbers|    Soccer|
    |   3|   Pittsburgh|   PA|  12|   Pittsburgh|Steelers|  Football|
    |null|         null| null|   9|      Seattle|Mariners|  Baseball|
    |null|         null| null|  10|      Seattle|Sounders|    Soccer|
    |   4|      Buffalo|   NY|   5|      Buffalo|   Bills|  Football|
    |   6|     New York|   NY|   3|     New York|  Giants|  Football|
    +----+-------------+-----+----+-------------+--------+----------+
    
    




    fullouterjoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 5 more fields]
    



## Cross Join

* A `crossJoin` join forms a cartesian join where every element on the left is associated on the right
* Analogous to a nested for-loop in programming
* Perhaps we want to build a baseball schedule for the next season


```scala
val baseballTeams = teams.where("sport_type = 'Baseball'")
val crossJoin = baseballTeams.crossJoin(baseballTeams)
crossJoin.show()
```

    +---+-------------+-------+----------+---+-------------+--------+----------+
    | id|         city|   team|sport_type| id|         city|    team|sport_type|
    +---+-------------+-------+----------+---+-------------+--------+----------+
    |  2|  Los Angeles|Dodgers|  Baseball|  2|  Los Angeles| Dodgers|  Baseball|
    |  2|  Los Angeles|Dodgers|  Baseball|  4|San Francisco|  Giants|  Baseball|
    |  2|  Los Angeles|Dodgers|  Baseball|  6|    Pittsburg| Pirates|  Baseball|
    |  2|  Los Angeles|Dodgers|  Baseball|  8|    San Diego|  Padres|  Baseball|
    |  2|  Los Angeles|Dodgers|  Baseball|  9|      Seattle|Mariners|  Baseball|
    |  4|San Francisco| Giants|  Baseball|  2|  Los Angeles| Dodgers|  Baseball|
    |  4|San Francisco| Giants|  Baseball|  4|San Francisco|  Giants|  Baseball|
    |  4|San Francisco| Giants|  Baseball|  6|    Pittsburg| Pirates|  Baseball|
    |  4|San Francisco| Giants|  Baseball|  8|    San Diego|  Padres|  Baseball|
    |  4|San Francisco| Giants|  Baseball|  9|      Seattle|Mariners|  Baseball|
    |  6|    Pittsburg|Pirates|  Baseball|  2|  Los Angeles| Dodgers|  Baseball|
    |  6|    Pittsburg|Pirates|  Baseball|  4|San Francisco|  Giants|  Baseball|
    |  6|    Pittsburg|Pirates|  Baseball|  6|    Pittsburg| Pirates|  Baseball|
    |  6|    Pittsburg|Pirates|  Baseball|  8|    San Diego|  Padres|  Baseball|
    |  6|    Pittsburg|Pirates|  Baseball|  9|      Seattle|Mariners|  Baseball|
    |  8|    San Diego| Padres|  Baseball|  2|  Los Angeles| Dodgers|  Baseball|
    |  8|    San Diego| Padres|  Baseball|  4|San Francisco|  Giants|  Baseball|
    |  8|    San Diego| Padres|  Baseball|  6|    Pittsburg| Pirates|  Baseball|
    |  8|    San Diego| Padres|  Baseball|  8|    San Diego|  Padres|  Baseball|
    |  8|    San Diego| Padres|  Baseball|  9|      Seattle|Mariners|  Baseball|
    +---+-------------+-------+----------+---+-------------+--------+----------+
    only showing top 20 rows
    
    




    baseballTeams: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id: int, city: string ... 2 more fields]
    crossJoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 6 more fields]
    



### Those column names are a bit confusing and repetitive


```scala
val newColumnNames = Seq("home_id", "home_city", "home_team", "home_sport_type", 
                         "away_id", "away_city", "away_team", "away_sport_type")
val crossJoinWithNewColumnNames = crossJoin.toDF(newColumnNames:_*)
crossJoinWithNewColumnNames.show(100)
```

    +-------+-------------+---------+---------------+-------+-------------+---------+---------------+
    |home_id|    home_city|home_team|home_sport_type|away_id|    away_city|away_team|away_sport_type|
    +-------+-------------+---------+---------------+-------+-------------+---------+---------------+
    |      2|  Los Angeles|  Dodgers|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      9|      Seattle| Mariners|       Baseball|
    +-------+-------------+---------+---------------+-------+-------------+---------+---------------+
    
    




    baseballTeams: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id: int, city: string ... 2 more fields]
    crossJoin: org.apache.spark.sql.DataFrame = [id: int, city: string ... 6 more fields]
    newColumnNames: Seq[String] = List(home_id, home_city, home_team, home_sport_type, away_id, away_city, away_team, away_sport_type)
    crossJoinWithNewColumnNames: org.apache.spark.sql.DataFrame = [home_id: int, home_city: string ... 6 more fields]
    



## Remove all the rows where there is the same team for both


```scala
val removedSameTeam = crossJoinWithNewColumnNames.where("away_team != home_team")
removedSameTeam.show(100)
```

    +-------+-------------+---------+---------------+-------+-------------+---------+---------------+
    |home_id|    home_city|home_team|home_sport_type|away_id|    away_city|away_team|away_sport_type|
    +-------+-------------+---------+---------------+-------+-------------+---------+---------------+
    |      2|  Los Angeles|  Dodgers|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      2|  Los Angeles|  Dodgers|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      4|San Francisco|   Giants|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      8|    San Diego|   Padres|       Baseball|
    |      6|    Pittsburg|  Pirates|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      8|    San Diego|   Padres|       Baseball|      9|      Seattle| Mariners|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      2|  Los Angeles|  Dodgers|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      4|San Francisco|   Giants|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      6|    Pittsburg|  Pirates|       Baseball|
    |      9|      Seattle| Mariners|       Baseball|      8|    San Diego|   Padres|       Baseball|
    +-------+-------------+---------+---------------+-------+-------------+---------+---------------+
    
    




    removedSameTeam: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [home_id: int, home_city: string ... 6 more fields]
    



## Removing some Useless Columns


```scala
val cleanTeams = removedSameTeam.drop("home_id", "home_sport_type", "away_id", "away_sport_type")
cleanTeams.show()
```

    +-------------+---------+-------------+---------+
    |    home_city|home_team|    away_city|away_team|
    +-------------+---------+-------------+---------+
    |  Los Angeles|  Dodgers|San Francisco|   Giants|
    |  Los Angeles|  Dodgers|    Pittsburg|  Pirates|
    |  Los Angeles|  Dodgers|    San Diego|   Padres|
    |  Los Angeles|  Dodgers|      Seattle| Mariners|
    |San Francisco|   Giants|  Los Angeles|  Dodgers|
    |San Francisco|   Giants|    Pittsburg|  Pirates|
    |San Francisco|   Giants|    San Diego|   Padres|
    |San Francisco|   Giants|      Seattle| Mariners|
    |    Pittsburg|  Pirates|  Los Angeles|  Dodgers|
    |    Pittsburg|  Pirates|San Francisco|   Giants|
    |    Pittsburg|  Pirates|    San Diego|   Padres|
    |    Pittsburg|  Pirates|      Seattle| Mariners|
    |    San Diego|   Padres|  Los Angeles|  Dodgers|
    |    San Diego|   Padres|San Francisco|   Giants|
    |    San Diego|   Padres|    Pittsburg|  Pirates|
    |    San Diego|   Padres|      Seattle| Mariners|
    |      Seattle| Mariners|  Los Angeles|  Dodgers|
    |      Seattle| Mariners|San Francisco|   Giants|
    |      Seattle| Mariners|    Pittsburg|  Pirates|
    |      Seattle| Mariners|    San Diego|   Padres|
    +-------------+---------+-------------+---------+
    
    




    cleanTeams: org.apache.spark.sql.DataFrame = [home_city: string, home_team: string ... 2 more fields]
    
