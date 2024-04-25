import time
import mysql.connector

# Establish a connection to the MySQL database
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="library1m"
)
print("Connected to MySQL database!")

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

# Define an array of queries
queries = [
    # Query 1
    """
    SELECT * FROM books;
    """,
    # Query 2
    """
    SELECT title, author, publication_date
        FROM books
        WHERE publication_date LIKE '2021%';
    """,
    # Query 3
    """
    SELECT b.title, b.author, bo.name
    FROM books b
    JOIN borrowing_history bh ON b.book_id = bh.book_id
    JOIN borrowers bo ON bo.borrower_id = bh.borrower_id
    WHERE DATE_FORMAT(bh.borrow_date, '%Y') = '2023';
    """,
    # Query 4
    """
    SELECT b.title, b.author, bh.borrow_date
    FROM books b
    JOIN borrowing_history bh ON b.book_id = bh.book_id
    WHERE YEAR(bh.borrow_date) = 2022
    GROUP BY b.title, b.author, bh.borrow_date
    HAVING COUNT(*) > (
        SELECT AVG(borrows_per_book)
        FROM (
            SELECT COUNT(*) AS borrows_per_book
            FROM borrowing_history
            WHERE YEAR(borrow_date) = 2022
            GROUP BY book_id
        ) AS subquery
    )
    AND b.author = 'Rebecca Patterson'
    ORDER BY bh.borrow_date DESC;
    """
]

# Define the number of times to execute each query
num_executions = 31

# Iterate through the queries and execute each one
for i, query in enumerate(queries):
    print(f"Executing query {i+1}/{len(queries)}...")

    # Open a new file for writing with a different name for each query
    with open(f"runtime1m_q{i+1}.txt", "w") as file:
        # Execute the query and record the runtime for each execution
        for j in range(num_executions):
            start_time = time.time()
            cursor.execute(query)
            result = cursor.fetchall()
            end_time = time.time()
            runtime = end_time - start_time

            # Write the runtime to the file
            file.write(f"{j+1}. {runtime:.4f} sec\n")

    print(f"Query {i+1} done!")

# Close the cursor and connection
cursor.close()
print("Cursor closed!")
connection.close()
print("Connection closed!")