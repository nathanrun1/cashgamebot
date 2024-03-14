import datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd

"""
This file acts as main database interaction for the bot.

Debt_history:
    For every element:
        Payer.balance -= Amount
        Recipient.balance += Amount 
Types:
    Payment: Payer settles debt. Recipient balance goes down, Payer balance goes up.
    Buy in & Cash out: Payer gains debt. Recipient balance goes up, Payer balance goes down.
"""

DATABASE_NAME = "cashgamebot"

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def make_database(connection):
    query = f"""
    CREATE DATABASE {DATABASE_NAME};
    
    USE {DATABASE_NAME};
    
    CREATE TABLE debt_history (
	    debt_type VARCHAR(20),
        recipient_id INT,
        payer_id INT,
        amount FLOAT,
        date DATE
    );

    CREATE TABLE player_data (
	    player_id INT,
        player_name VARCHAR(25),
        balance INT
    );
    """
    cursor = connection.cursor()
    cursor.execute(query)


def get_connection():
    return create_server_connection("localhost", "root", "iit7&&pORt")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def get_table(connection, table_name):
    execute_query(connection, "USE cashgamebot")
    query = f"SELECT * FROM {table_name}"
    try:
        pd_table = pd.read_sql(query, connection)
        return pd_table
    except Exception as e:
        print(str(e))


def update_player_balance(player_id, balance):
    connection = get_connection()
    execute_query(connection, "USE cashgamebot")
    query = (f"UPDATE player_data "
             f"SET balance = {balance} "
             f"WHERE player_id = {player_id}")
    execute_query(connection, query)
    connection.close()
    return balance


def add_debt(debt_type, recipient_id, payer_id, amount):
    connection = get_connection()
    execute_query(connection, "USE cashgamebot")
    query = (f"INSERT INTO debt_history (debt_type, recipient_id, payer_id, amount, date) VALUES "
             f"('{debt_type}', {recipient_id}, {payer_id}, {amount}, "
             f"'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')")
    print(query)
    execute_query(connection, query)
    connection.close()


def add_player(player_name):
    connection = get_connection()
    execute_query(connection, "USE cashgamebot")
    query = f"INSERT INTO player_data (player_name) VALUES ('{player_name}')"
    execute_query(connection, query)
    connection.close()

def calculate_player_balance(player_id):
    connection = get_connection()
    for debt in get_table(connection, "debt_history"):
        if debt['recipient_id'] == player_id:







