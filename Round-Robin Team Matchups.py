# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Round-Robin Team Matchups
# MAGIC %md
# MAGIC # Round-Robin Team Matchups
# MAGIC
# MAGIC Generate every possible pairing where each team plays every other team exactly once (teams: A, B, C, D).

# COMMAND ----------

# DBTITLE 1,SQL: Round-Robin Matchups
# MAGIC %sql
# MAGIC -- SQL version: self-join with team1 < team2 to get each pairing once
# MAGIC WITH teams AS (
# MAGIC   SELECT explode(array('A', 'B', 'C', 'D')) AS team
# MAGIC )
# MAGIC SELECT
# MAGIC   t1.team AS team1,
# MAGIC   t2.team AS team2
# MAGIC FROM teams t1
# MAGIC CROSS JOIN teams t2
# MAGIC WHERE t1.team < t2.team
# MAGIC ORDER BY team1, team2

# COMMAND ----------

# DBTITLE 1,PySpark: Round-Robin Matchups
# PySpark version: self-join with team1 < team2 to get each pairing once
from pyspark.sql import functions as F

teams = spark.createDataFrame(
    [("A",), ("B",), ("C",), ("D",)],
    ["team"]
)

matches = teams.alias("t1").crossJoin(teams.alias("t2")).where(
    F.col("t1.team") < F.col("t2.team")
).select(
    F.col("t1.team").alias("team1"),
    F.col("t2.team").alias("team2")
).orderBy("team1", "team2")

display(matches)

# COMMAND ----------

teams.alias('a').join(teams.alias('b'), F.col('a.team') < F.col('b.team'), 'inner').select('a.team', 'b.team').show()

# COMMAND ----------

# DBTITLE 1,Q1: Same Department Pairs
# MAGIC %md
# MAGIC ## Q1: Same Department Pairs
# MAGIC
# MAGIC Given a table of employees `(emp_id, emp_name, department)`, find all pairs of employees who work in the **same department**.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | emp_name_1 | emp_name_2 | department |
# MAGIC |---|---|---|
# MAGIC | Alice | Bob | Sales |
# MAGIC | Alice | Grace | Sales |
# MAGIC | Bob | Grace | Sales |
# MAGIC | Charlie | Diana | Engineering |
# MAGIC | Charlie | Frank | Engineering |
# MAGIC | Diana | Frank | Engineering |
# MAGIC | Eve | Heidi | Marketing |

# COMMAND ----------

# DBTITLE 1,Q1: Sample Data
# Sample data for Q1
employees = spark.createDataFrame(
    [(1, "Alice", "Sales"),
     (2, "Bob", "Sales"),
     (3, "Charlie", "Engineering"),
     (4, "Diana", "Engineering"),
     (5, "Eve", "Marketing"),
     (6, "Frank", "Engineering"),
     (7, "Grace", "Sales"),
     (8, "Heidi", "Marketing")],
    ["emp_id", "emp_name", "department"]
)
employees.createOrReplaceTempView('employees')
display(employees)

# COMMAND ----------

# MAGIC %sql
# MAGIC select e1.emp_name, e2.emp_name, e1.department from employees e1
# MAGIC join employees e2
# MAGIC on e1.emp_name < e2.emp_name
# MAGIC and e1.department = e2.department
# MAGIC order by e1.emp_name, e2.emp_name;

# COMMAND ----------

from pyspark.sql import functions as f
employees.alias('e1').join(employees.alias('e2'), (F.col('e1.emp_name') < F.col('e2.emp_name')) & (F.col('e1.department') == F.col('e2.department')), 'inner')\
    .select(f.col('e1.emp_name').alias('emp1'), f.col('e2.emp_name').alias('emp2'), f.col('e2.department'))\
    .orderBy('emp1', 'emp2')\
    .show()

# COMMAND ----------

# DBTITLE 1,Solution Q1: Same Department Pairs
# Solution Q1 — pairs of employees in the same department
# Equality key (department) enables a hash join; name_1 < name_2 keeps each pair once.
from pyspark.sql.functions import col

employees = spark.createDataFrame(
    [(1, "Alice", "Sales"), (2, "Bob", "Sales"), (3, "Charlie", "Engineering"),
     (4, "Diana", "Engineering"), (5, "Eve", "Marketing"), (6, "Frank", "Engineering"),
     (7, "Grace", "Sales"), (8, "Heidi", "Marketing")],
    ["emp_id", "emp_name", "department"]
)

display(
    employees.alias("a")
    .join(
        employees.alias("b"),
        (col("a.department") == col("b.department")) & (col("a.emp_name") < col("b.emp_name")),
    )
    .select(
        col("a.emp_name").alias("emp_name_1"),
        col("b.emp_name").alias("emp_name_2"),
        col("a.department").alias("department"),
    )
    .orderBy("emp_name_1", "emp_name_2")
)  # 7 pairs

# COMMAND ----------

# DBTITLE 1,Q2: Price Difference Pairs
# MAGIC %md
# MAGIC ## Q2: Price Difference Pairs
# MAGIC
# MAGIC Given a table of products `(product_id, product_name, price)`, find all pairs of products where the **price difference is more than 100**.
# MAGIC
# MAGIC **Expected Output (sample rows):**
# MAGIC
# MAGIC | product_name_1 | product_name_2 | price_diff |
# MAGIC |---|---|---|
# MAGIC | Laptop | Mouse | 1175 |
# MAGIC | Laptop | Keyboard | 1120 |
# MAGIC | Laptop | Monitor | 750 |
# MAGIC | Laptop | USB Cable | 1185 |
# MAGIC | Laptop | Webcam | 1110 |
# MAGIC | ... | ... | ... |
# MAGIC
# MAGIC *Total: 21 pairs where price difference > 100 (use abs(a.price - b.price) > 100)*

# COMMAND ----------

# DBTITLE 1,Q2: Sample Data
# Sample data for Q2
products = spark.createDataFrame(
    [(1, "Laptop", 1200),
     (2, "Mouse", 25),
     (3, "Keyboard", 80),
     (4, "Monitor", 450),
     (5, "USB Cable", 15),
     (6, "Webcam", 90),
     (7, "Headphones", 200),
     (8, "Docking Station", 350)],
    ["product_id", "product_name", "price"]
)
products.createOrReplaceTempView('products')
display(products)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.product_name product_1, b.product_name product_2, a.price product_1_price, b.price product_2_price, a.price - b.price price_diff from products a, products b where a.price - b.price > 100 and a.product_name < b.product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select p1.product_name as product_1, p2.product_name as product_2, p1.price - p2.price price_diff
# MAGIC from products p1
# MAGIC join products p2
# MAGIC on p1.price - p2.price > 100
# MAGIC and p1.product_name < p2.product_name;

# COMMAND ----------

from pyspark.sql.functions import *
display(products.alias('a').join(products.alias('b'), (col('a.product_name') < col('b.product_name')) & (abs(col('a.price') - col('b.price')) > 100), 'inner')\
    .select(col('a.product_name').alias('product_1'), col('b.product_name').alias('product_2'), (abs(col('a.price') - col('b.price'))).alias('price_diff')))

# COMMAND ----------

# DBTITLE 1,Solution Q2: Price Difference Pairs
# Solution Q2 — pairs of products with price difference > 100
# Dedup on ONE column (product_name <); abs() makes the direction of the price diff irrelevant.
from pyspark.sql.functions import col, abs

products = spark.createDataFrame(
    [(1, "Laptop", 1200), (2, "Mouse", 25), (3, "Keyboard", 80), (4, "Monitor", 450),
     (5, "USB Cable", 15), (6, "Webcam", 90), (7, "Headphones", 200), (8, "Docking Station", 350)],
    ["product_id", "product_name", "price"]
)

display(
    products.alias("a")
    .join(
        products.alias("b"),
        (col("a.product_name") < col("b.product_name"))
        & (abs(col("a.price") - col("b.price")) > 100),
    )
    .select(
        col("a.product_name").alias("product_name_1"),
        col("b.product_name").alias("product_name_2"),
        abs(col("a.price") - col("b.price")).alias("price_diff"),
    )
    .orderBy("product_name_1", "product_name_2")
)  # 21 pairs

# COMMAND ----------

# DBTITLE 1,Q3: Different State Cities
# MAGIC %md
# MAGIC ## Q3: Different State Cities
# MAGIC
# MAGIC Given a table of cities `(city_id, city_name, state)`, find all pairs of cities in **different states**.
# MAGIC
# MAGIC **Expected Output (sample rows):**
# MAGIC
# MAGIC | city_name_1 | city_name_2 |
# MAGIC |---|---|
# MAGIC | Austin | Seattle |
# MAGIC | Austin | Spokane |
# MAGIC | Austin | Miami |
# MAGIC | Austin | Orlando |
# MAGIC | Austin | Denver |
# MAGIC | ... | ... |
# MAGIC
# MAGIC *Total: 24 pairs where the two cities are in different states*

# COMMAND ----------

# DBTITLE 1,Q3: Sample Data
# Sample data for Q3
cities = spark.createDataFrame(
    [(1, "Austin", "Texas"),
     (2, "Dallas", "Texas"),
     (3, "Seattle", "Washington"),
     (4, "Spokane", "Washington"),
     (5, "Miami", "Florida"),
     (6, "Orlando", "Florida"),
     (7, "Denver", "Colorado"),
     (8, "Boulder", "Colorado")],
    ["city_id", "city_name", "state"]
)
cities.createTempView('Cities')
display(cities)

# COMMAND ----------

cities.alias('a').join(cities.alias('b'), (col('a.state') != col('b.state')) & (col('a.city_name') < col('b.city_name')), 'inner')\
    .select(col('a.city_name').alias('city_name_1'), col('b.city_name').alias('city_name_2')).orderBy('city_name_1', 'city_name_2').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.city_name city_name_1, b.city_name city_name_2 from cities a, cities b where a.city_name < b.city_name and a.state <> b.state;

# COMMAND ----------

# DBTITLE 1,Solution Q3: Different State Cities
# Solution Q3 — pairs of cities in DIFFERENT states
# Dedup with city_name <; the state test must be != (never state < state — the two
# alphabetical orderings disagree and valid pairs get dropped, e.g. Austin–Dallas).
from pyspark.sql.functions import col

cities = spark.createDataFrame(
    [(1, "Austin", "Texas"), (2, "Dallas", "Texas"), (3, "Seattle", "Washington"),
     (4, "Spokane", "Washington"), (5, "Miami", "Florida"), (6, "Orlando", "Florida"),
     (7, "Denver", "Colorado"), (8, "Boulder", "Colorado")],
    ["city_id", "city_name", "state"]
)

display(
    cities.alias("a")
    .join(
        cities.alias("b"),
        (col("a.city_name") < col("b.city_name")) & (col("a.state") != col("b.state")),
    )
    .select(
        col("a.city_name").alias("city_name_1"),
        col("b.city_name").alias("city_name_2"),
    )
    .orderBy("city_name_1", "city_name_2")
)  # 24 pairs

# COMMAND ----------

# DBTITLE 1,Q4: Same Grade Students
# MAGIC %md
# MAGIC ## Q4: Same Grade Students
# MAGIC
# MAGIC Given a table of students `(student_id, student_name, grade)`, find all pairs of students with the **same grade**.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | student_name_1 | student_name_2 | grade |
# MAGIC |---|---|---|
# MAGIC | Alice | Charlie | A |
# MAGIC | Alice | Frank | A |
# MAGIC | Charlie | Frank | A |
# MAGIC | Bob | Eve | B |
# MAGIC | Bob | Grace | B |
# MAGIC | Eve | Grace | B |
# MAGIC | Diana | Heidi | C |

# COMMAND ----------

# DBTITLE 1,Q4: Sample Data
# Sample data for Q4
students = spark.createDataFrame(
    [(1, "Alice", "A"),
     (2, "Bob", "B"),
     (3, "Charlie", "A"),
     (4, "Diana", "C"),
     (5, "Eve", "B"),
     (6, "Frank", "A"),
     (7, "Grace", "B"),
     (8, "Heidi", "C")],
    ["student_id", "student_name", "grade"]
)
students.createOrReplaceTempView('Students')
display(students)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.student_name student_name_1, b.student_name student_name_2, a.grade from students a, students b where a.student_name < b.student_name and a.grade = b.grade order by a.grade;

# COMMAND ----------

students.alias('a').join(students.alias('b'), (col('a.grade') == col('b.grade')) & (col('a.student_name') < col('b.student_name')), 'inner')\
    .select(col('a.student_name').alias('student_name_1'), col('b.student_name').alias('student_name_2'), 'a.grade').orderBy('grade').show()

# COMMAND ----------

# DBTITLE 1,Solution Q4: Same Grade Students
# Solution Q4 — pairs of students with the same grade
from pyspark.sql.functions import col

students = spark.createDataFrame(
    [(1, "Alice", "A"), (2, "Bob", "B"), (3, "Charlie", "A"), (4, "Diana", "C"),
     (5, "Eve", "B"), (6, "Frank", "A"), (7, "Grace", "B"), (8, "Heidi", "C")],
    ["student_id", "student_name", "grade"]
)

display(
    students.alias("a")
    .join(
        students.alias("b"),
        (col("a.grade") == col("b.grade")) & (col("a.student_name") < col("b.student_name")),
    )
    .select(
        col("a.student_name").alias("student_name_1"),
        col("b.student_name").alias("student_name_2"),
        col("a.grade").alias("grade"),
    )
    .orderBy("grade", "student_name_1", "student_name_2")
)  # 7 pairs

# COMMAND ----------

# DBTITLE 1,Q5: Cross-Team Player Matchups
# MAGIC %md
# MAGIC ## Q5: Cross-Team Player Matchups
# MAGIC
# MAGIC Given a table of players `(player_id, player_name, team)`, generate a schedule where every player plays against every other player **on a different team**.
# MAGIC
# MAGIC **Expected Output (sample rows):**
# MAGIC
# MAGIC | player_name_1 | player_name_2 | team_1 | team_2 |
# MAGIC |---|---|---|---|
# MAGIC | LeBron | Stephen | Lakers | Warriors |
# MAGIC | LeBron | Klay | Lakers | Warriors |
# MAGIC | LeBron | Kevin | Lakers | Suns |
# MAGIC | LeBron | Devin | Lakers | Suns |
# MAGIC | LeBron | Giannis | Lakers | Bucks |
# MAGIC | ... | ... | ... | ... |
# MAGIC
# MAGIC *Total: 24 matchups (players from different teams only)*

# COMMAND ----------

# DBTITLE 1,Q5: Sample Data
# Sample data for Q5
players = spark.createDataFrame(
    [(1, "LeBron", "Lakers"),
     (2, "Anthony", "Lakers"),
     (3, "Stephen", "Warriors"),
     (4, "Klay", "Warriors"),
     (5, "Kevin", "Suns"),
     (6, "Devin", "Suns"),
     (7, "Giannis", "Bucks"),
     (8, "Jrue", "Bucks")],
    ["player_id", "player_name", "team"]
)
players.createOrReplaceTempView('players')
display(players)

# COMMAND ----------

# MAGIC %sql
# MAGIC select p1.player_name player_name_1, p2.player_name player_name_2, p1.team team_1, p2.team team_2 from players p1, players p2 where p1.player_name < p2.player_name and p1.team <> p2.team
# MAGIC order by p1.player_name, p2.player_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select p1.player_name player_name_1, p2.player_name player_name_2, p1.team team_1, p2.team team_2 from players p1 cross join players p2 where p1.player_name < p2.player_name and p1.team <> p2.team
# MAGIC order by p1.player_name, p2.player_name;

# COMMAND ----------

display(players.alias('a').join(players.alias('b'), (col('a.player_name') < col('b.player_name')) & (col('a.team') != col('b.team')), 'inner')\
    .select(col('a.player_name').alias('player_1'), col('b.player_name').alias('player_2'), col('a.team').alias('team_1'), col('b.team').alias('team_2')).orderBy('player_1'))

# COMMAND ----------

# DBTITLE 1,Solution Q5: Cross-Team Player Matchups
# Solution Q5 — matchups between players on DIFFERENT teams
# Dedup with player_name <; the team test is != (team < team drops valid pairs
# like LeBron–Klay, where name order and team order disagree).
from pyspark.sql.functions import col

players = spark.createDataFrame(
    [(1, "LeBron", "Lakers"), (2, "Anthony", "Lakers"), (3, "Stephen", "Warriors"),
     (4, "Klay", "Warriors"), (5, "Kevin", "Suns"), (6, "Devin", "Suns"),
     (7, "Giannis", "Bucks"), (8, "Jrue", "Bucks")],
    ["player_id", "player_name", "team"]
)

display(
    players.alias("a")
    .join(
        players.alias("b"),
        (col("a.player_name") < col("b.player_name")) & (col("a.team") != col("b.team")),
    )
    .select(
        col("a.player_name").alias("player_name_1"),
        col("b.player_name").alias("player_name_2"),
        col("a.team").alias("team_1"),
        col("b.team").alias("team_2"),
    )
    .orderBy("player_name_1", "player_name_2")
)  # 24 matchups

# COMMAND ----------

# DBTITLE 1,Q6: Same Location Departments
# MAGIC %md
# MAGIC ## Q6: Same Location Departments
# MAGIC
# MAGIC Given a table of departments `(dept_id, dept_name, location)`, find all pairs of departments in the **same location**.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | dept_name_1 | dept_name_2 | location |
# MAGIC |---|---|---|
# MAGIC | HR | Finance | New York |
# MAGIC | HR | Legal | New York |
# MAGIC | Finance | Legal | New York |
# MAGIC | Engineering | QA | San Francisco |
# MAGIC | Sales | Marketing | Chicago |

# COMMAND ----------

# DBTITLE 1,Q6: Sample Data
# Sample data for Q6
departments = spark.createDataFrame(
    [(1, "HR", "New York"),
     (2, "Finance", "New York"),
     (3, "Engineering", "San Francisco"),
     (4, "QA", "San Francisco"),
     (5, "Sales", "Chicago"),
     (6, "Marketing", "Chicago"),
     (7, "Legal", "New York"),
     (8, "Operations", "Austin")],
    ["dept_id", "dept_name", "location"]
)
departments.createOrReplaceTempView('departments')
display(departments)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.dept_name dept_name_1, b.dept_name dept_name_2, a.location from departments a cross join departments b where a.location  = b.location and a.dept_name < b.dept_name;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

departments.alias('a').join(departments.alias('b'), (col('a.location') == col('b.location')) & (col('a.dept_name')<col('b.dept_name')), 'inner')\
    .select(col('a.dept_name').alias('dept_name_1'), (col('b.dept_name').alias('dept_name_2')), 'a.location').show()

# COMMAND ----------

# DBTITLE 1,Solution Q6: Same Location Departments
# Solution Q6 — pairs of departments in the same location
from pyspark.sql.functions import col

departments = spark.createDataFrame(
    [(1, "HR", "New York"), (2, "Finance", "New York"), (3, "Engineering", "San Francisco"),
     (4, "QA", "San Francisco"), (5, "Sales", "Chicago"), (6, "Marketing", "Chicago"),
     (7, "Legal", "New York"), (8, "Operations", "Austin")],
    ["dept_id", "dept_name", "location"]
)

display(
    departments.alias("a")
    .join(
        departments.alias("b"),
        (col("a.location") == col("b.location")) & (col("a.dept_name") < col("b.dept_name")),
    )
    .select(
        col("a.dept_name").alias("dept_name_1"),
        col("b.dept_name").alias("dept_name_2"),
        col("a.location").alias("location"),
    )
    .orderBy("location", "dept_name_1", "dept_name_2")
)  # 5 pairs

# COMMAND ----------

# DBTITLE 1,Q7: Same Author Books
# MAGIC %md
# MAGIC ## Q7: Same Author Books
# MAGIC
# MAGIC Given a table of books `(book_id, title, author)`, find all pairs of books by the **same author**.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | title_1 | title_2 | author |
# MAGIC |---|---|---|
# MAGIC | The Hobbit | The Fellowship | Tolkien |
# MAGIC | The Hobbit | The Two Towers | Tolkien |
# MAGIC | The Fellowship | The Two Towers | Tolkien |
# MAGIC | Harry Potter 1 | Harry Potter 2 | Rowling |
# MAGIC | 1984 | Animal Farm | Orwell |

# COMMAND ----------

# DBTITLE 1,Q7: Sample Data
# Sample data for Q7
books = spark.createDataFrame(
    [(1, "The Hobbit", "Tolkien"),
     (2, "The Fellowship", "Tolkien"),
     (3, "The Two Towers", "Tolkien"),
     (4, "Harry Potter 1", "Rowling"),
     (5, "Harry Potter 2", "Rowling"),
     (6, "Dune", "Herbert"),
     (7, "1984", "Orwell"),
     (8, "Animal Farm", "Orwell")],
    ["book_id", "title", "author"]
)
books.createOrReplaceTempView('books')
display(books)

# COMMAND ----------

books.alias('a').join(books.alias('b'), (col('a.author') == col('b.author')) & (col('a.title') < col('b.title')), 'inner')\
    .select(col('a.title').alias('title_1'),col('a.title').alias('title_2'), 'a.author').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.title title_1, b.title title_2, a.author from books a cross join books b where a.author = b.author and a.title < b.title;

# COMMAND ----------

# DBTITLE 1,Solution Q7: Same Author Books
# Solution Q7 — pairs of books by the same author
# Note: title_2 must come from alias "b" (col("b.title")), not col("a.title").
from pyspark.sql.functions import col

books = spark.createDataFrame(
    [(1, "The Hobbit", "Tolkien"), (2, "The Fellowship", "Tolkien"), (3, "The Two Towers", "Tolkien"),
     (4, "Harry Potter 1", "Rowling"), (5, "Harry Potter 2", "Rowling"), (6, "Dune", "Herbert"),
     (7, "1984", "Orwell"), (8, "Animal Farm", "Orwell")],
    ["book_id", "title", "author"]
)

display(
    books.alias("a")
    .join(
        books.alias("b"),
        (col("a.author") == col("b.author")) & (col("a.title") < col("b.title")),
    )
    .select(
        col("a.title").alias("title_1"),
        col("b.title").alias("title_2"),
        col("a.author").alias("author"),
    )
    .orderBy("author", "title_1", "title_2")
)  # 5 pairs

# COMMAND ----------

# DBTITLE 1,Q8: Different City Team Matchups
# MAGIC %md
# MAGIC ## Q8: Different City Team Matchups
# MAGIC
# MAGIC Given a table of teams `(team_id, team_name, city)`, generate matchups between teams in **different cities** only.
# MAGIC
# MAGIC **Expected Output (sample rows):**
# MAGIC
# MAGIC | team_name_1 | team_name_2 | city_1 | city_2 |
# MAGIC |---|---|---|---|
# MAGIC | Lakers | Warriors | Los Angeles | San Francisco |
# MAGIC | Lakers | Kings | Los Angeles | Sacramento |
# MAGIC | Lakers | Nuggets | Los Angeles | Denver |
# MAGIC | Lakers | Suns | Los Angeles | Phoenix |
# MAGIC | Lakers | Mavericks | Los Angeles | Dallas |
# MAGIC | ... | ... | ... | ... |
# MAGIC
# MAGIC *Total: 27 matchups (teams from different cities only; Lakers-Clippers excluded — same city)*

# COMMAND ----------

# DBTITLE 1,Q8: Sample Data
# Sample data for Q8
teams_city = spark.createDataFrame(
    [(1, "Lakers", "Los Angeles"),
     (2, "Clippers", "Los Angeles"),
     (3, "Warriors", "San Francisco"),
     (4, "Kings", "Sacramento"),
     (5, "Nuggets", "Denver"),
     (6, "Suns", "Phoenix"),
     (7, "Mavericks", "Dallas"),
     (8, "Rockets", "Houston")],
    ["team_id", "team_name", "city"]
)
teams_city.createOrReplaceTempView('teams_city')
display(teams_city)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.team_name team_name_1, b.team_name team_name_2, a.city city_1, b.city city_2 from teams_city a, teams_city b where a.team_name < b.team_name and a.city <> b.city;

# COMMAND ----------

# DBTITLE 1,Solution Q8: Different City Team Matchups
# Solution Q8 — matchups between teams in DIFFERENT cities
# Dedup with team_name <; the city test is != (city < city drops valid pairs
# like Lakers–Rockets, where name order and city order disagree).
from pyspark.sql.functions import col

teams_city = spark.createDataFrame(
    [(1, "Lakers", "Los Angeles"), (2, "Clippers", "Los Angeles"), (3, "Warriors", "San Francisco"),
     (4, "Kings", "Sacramento"), (5, "Nuggets", "Denver"), (6, "Suns", "Phoenix"),
     (7, "Mavericks", "Dallas"), (8, "Rockets", "Houston")],
    ["team_id", "team_name", "city"]
)

display(
    teams_city.alias("a")
    .join(
        teams_city.alias("b"),
        (col("a.team_name") < col("b.team_name")) & (col("a.city") != col("b.city")),
    )
    .select(
        col("a.team_name").alias("team_name_1"),
        col("b.team_name").alias("team_name_2"),
        col("a.city").alias("city_1"),
        col("b.city").alias("city_2"),
    )
    .orderBy("team_name_1", "team_name_2")
)  # 27 matchups (Lakers–Clippers excluded: same city)

# COMMAND ----------

# DBTITLE 1,Q9: Different Country Customers
# MAGIC %md
# MAGIC ## Q9: Different Country Customers
# MAGIC
# MAGIC Given a table of customers `(customer_id, customer_name, country)`, find all pairs of customers from **different countries**.
# MAGIC
# MAGIC **Expected Output (sample rows):**
# MAGIC
# MAGIC | customer_name_1 | customer_name_2 | country_1 | country_2 |
# MAGIC |---|---|---|---|
# MAGIC | Alice | Charlie | USA | Canada |
# MAGIC | Alice | Diana | USA | Canada |
# MAGIC | Alice | Eve | USA | UK |
# MAGIC | Alice | Frank | USA | UK |
# MAGIC | Alice | Grace | USA | India |
# MAGIC | ... | ... | ... | ... |
# MAGIC
# MAGIC *Total: 24 pairs where the two customers are from different countries*

# COMMAND ----------

# DBTITLE 1,Q9: Sample Data
# Sample data for Q9
customers = spark.createDataFrame(
    [(1, "Alice", "USA"),
     (2, "Bob", "USA"),
     (3, "Charlie", "Canada"),
     (4, "Diana", "Canada"),
     (5, "Eve", "UK"),
     (6, "Frank", "UK"),
     (7, "Grace", "India"),
     (8, "Heidi", "India")],
    ["customer_id", "customer_name", "country"]
)
customers.createTempView('customers')
display(customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.customer_name customer_1, b.customer_name customer_2, a.country country_1, b.country country_2 from customers a cross join customers b on a.customer_name < b.customer_name and a.country <> b.country;

# COMMAND ----------

# DBTITLE 1,Solution Q9: Different Country Customers
# Solution Q9 — pairs of customers from DIFFERENT countries
# customer_name < for dedup; country != for the "different" condition.
from pyspark.sql.functions import col

customers = spark.createDataFrame(
    [(1, "Alice", "USA"), (2, "Bob", "USA"), (3, "Charlie", "Canada"), (4, "Diana", "Canada"),
     (5, "Eve", "UK"), (6, "Frank", "UK"), (7, "Grace", "India"), (8, "Heidi", "India")],
    ["customer_id", "customer_name", "country"]
)

display(
    customers.alias("a")
    .join(
        customers.alias("b"),
        (col("a.customer_name") < col("b.customer_name")) & (col("a.country") != col("b.country")),
    )
    .select(
        col("a.customer_name").alias("customer_name_1"),
        col("b.customer_name").alias("customer_name_2"),
        col("a.country").alias("country_1"),
        col("b.country").alias("country_2"),
    )
    .orderBy("customer_name_1", "customer_name_2")
)  # 24 pairs

# COMMAND ----------

# DBTITLE 1,Q10: Same Genre Movies
# MAGIC %md
# MAGIC ## Q10: Same Genre Movies
# MAGIC
# MAGIC Given a table of movies `(movie_id, title, genre)`, find all pairs of movies in the **same genre** ordered by title.
# MAGIC
# MAGIC **Expected Output:**
# MAGIC
# MAGIC | title_1 | title_2 | genre |
# MAGIC |---|---|---|
# MAGIC | Die Hard | John Wick | Action |
# MAGIC | Get Out | Hereditary | Horror |
# MAGIC | Get Out | The Conjuring | Horror |
# MAGIC | Hereditary | The Conjuring | Horror |
# MAGIC | Inception | Interstellar | Sci-Fi |
# MAGIC | Inception | The Matrix | Sci-Fi |
# MAGIC | Interstellar | The Matrix | Sci-Fi |

# COMMAND ----------

# DBTITLE 1,Q10: Sample Data
# Sample data for Q10
movies = spark.createDataFrame(
    [(1, "Inception", "Sci-Fi"),
     (2, "Interstellar", "Sci-Fi"),
     (3, "The Matrix", "Sci-Fi"),
     (4, "Die Hard", "Action"),
     (5, "John Wick", "Action"),
     (6, "The Conjuring", "Horror"),
     (7, "Hereditary", "Horror"),
     (8, "Get Out", "Horror")],
    ["movie_id", "title", "genre"]
)
movies.createOrReplaceTempView('movies')
display(movies)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.title title_1, b.title title_2, a.genre from movies a, movies b where (a.title < b.title) and (a.genre = b.genre);

# COMMAND ----------

# DBTITLE 1,Solution Q10: Same Genre Movies
# Solution Q10 — pairs of movies in the same genre, ordered by title
from pyspark.sql.functions import col

movies = spark.createDataFrame(
    [(1, "Inception", "Sci-Fi"), (2, "Interstellar", "Sci-Fi"), (3, "The Matrix", "Sci-Fi"),
     (4, "Die Hard", "Action"), (5, "John Wick", "Action"), (6, "The Conjuring", "Horror"),
     (7, "Hereditary", "Horror"), (8, "Get Out", "Horror")],
    ["movie_id", "title", "genre"]
)

display(
    movies.alias("a")
    .join(
        movies.alias("b"),
        (col("a.genre") == col("b.genre")) & (col("a.title") < col("b.title")),
    )
    .select(
        col("a.title").alias("title_1"),
        col("b.title").alias("title_2"),
        col("a.genre").alias("genre"),
    )
    .orderBy("title_1", "title_2")
)  # 7 pairs