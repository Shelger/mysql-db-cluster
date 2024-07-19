import mysql.connector
from mysql.connector import Error
import time
import threading

# Create a lock object
lock = threading.Lock()

def create_connection(host, port):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user='shelger',
            password='960418',
            database='mydb'
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def perform_write_queries(connection, name):
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO users (name) VALUES (%s)", (name,))
        connection.commit()
    except Error as e:
        print(f"Error: {e}")

def perform_read_queries(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Error as e:
        print(f"Error: {e}")

def perform_delete_queries(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM users")
        connection.commit()
    except Error as e:
        print(f"Error: {e}")

def write_load():
    iterations = 10
    for i in range(iterations):
        connection = create_connection('localhost', 3309)
        if connection:
            with lock:
                perform_write_queries(connection, f'test{i}')
            connection.close()
        time.sleep(0.1)

def read_load():
    iterations = 10
    for _ in range(iterations):
        connection = create_connection('localhost', 3310)
        if connection:
            with lock:
                perform_read_queries(connection)
            connection.close()
        time.sleep(0.1)

def delete_load():
    while True:
        connection = create_connection('localhost', 3309)
        if connection:
            with lock:
                perform_delete_queries(connection)
            connection.close()
        time.sleep(60)

def main():
    # write_thread = threading.Thread(target=write_load)
    # read_thread = threading.Thread(target=read_load)
    delete_thread = threading.Thread(target=delete_load)

    # write_thread.start()
    # read_thread.start()
    delete_thread.start()

    # write_thread.join()
    # read_thread.join()
    delete_thread.join()

if __name__ == "__main__":
    main()

