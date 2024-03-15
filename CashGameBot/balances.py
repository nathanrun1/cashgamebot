import datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd

# --- READ ME! ---:
# This file acts as main database interaction for the bot.
#
# TO USE THE DATABASE:
#
# for usage in file, write:
# 'from balances import Balances'
#
# Balances is the class used for interaction with the MySQL database
#
# RELEVANT FUNCTIONS:
# Balances.get_players() returns list of all players as Player objects (holding their name and balance)
# Balances.get_debts() returns list with full debt history as Debt objects (including debt_type, recipient_id,
#     payer_id, amount, date)
# Balances.get_player(player_id) gets player with id 'player_id', returns as a Player object
# Balances.update_player_balance(player_id, balance) sets new balance to 'balance' for player with id 'player_id'
# Balances.add_player_net(player_id, amount) adds 'amount' to total net gains of player with id 'player_id'
# Balances.update_player_net(player_id, net) sets new total net gains to 'net' for player with id 'player_id'
# Balances.add_player_net(player_id, amount) adds 'amount' to balance of player with id 'player_id'
# Balances.add_player(player_name) adds a player of name 'player_name' to the database
# Balances.add_debt(debt_type, recipient_id, payer_id, amount) adds a new debt to database with provided parameters
# Balances.refresh_balances() recalculates and updates all player balances based solely on debt history


# ------------- CONSTANTS -------------


DATABASE_NAME = "cashgamebot"


# ------------- FUNCTIONS -------------


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


def get_connection():
    return create_server_connection("localhost", "root", "iit7&&pORt")


def __make_database(connection):
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


# ------------- CLASSES -------------


# class Player contains a player's player_id, name, balance and net winnings. Used to compare different players and
#   balances with ease
# Initialize: plr = Player(player_id, name, balance, net)
class Player:
    def __init__(self, player_id, name, balance, net):
        self.player_id = player_id
        self.name = name
        self.balance = balance
        self.net = net

    def __str__(self):
        return (f'{self.player_id} - {self.name} - '
                f'{f"${self.balance}" if self.balance >= 0 else f"(${-self.balance})"}')

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.player_id == other.player_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.net < other.net

    def __le__(self, other):
        return self.net <= other.net

    def __gt__(self, other):
        return self.net > other.net

    def __ge__(self, other):
        return self.net >= other.net


# class Debt contains a debt's debt_type, recipient_id, payer_id, amount and date added. Used to interact
#   with individual debts more easily.
# Initialize: debt = Debt(debt_type, recipient_id, payer_id, amount, date)
class Debt:
    def __init__(self, debt_type, recipient_id, payer_id, amount, date):
        self.debt_type = debt_type
        self.recipient_id = recipient_id
        self.payer_id = payer_id
        self.amount = amount
        self.date = date

    def __str__(self):
        return (f"Debt type: {self.debt_type}\n"
                f"Recipient id: {self.recipient_id}\n"
                f"Payer id: {self.payer_id}\n"
                f"Amount: {self.amount}\n"
                f"Date: {self.date}\n")

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.debt_type, self.recipient_id, self.payer_id, self.amount, self.date))

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        return not self.__eq__(other)


# class Balances allows direct interaction with the database.
# Initialize: balances = Balances()
class Balances:
    def __init__(self):
        self.connection = get_connection()

    def __execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            return cursor.rowcount
        except Error as err:
            print(f"Error: '{err}'")

    def __get_table(self, table_name):
        self.__execute_query("USE cashgamebot")
        query = f"SELECT * FROM {table_name}"
        try:
            pd_table = pd.read_sql(query, self.connection)
            return pd_table
        except Exception as e:
            print(str(e))

    def get_players(self):
        player_data = self.__get_table("player_data")
        players = []
        for i in player_data.index:
            player_id = player_data['player_id'][i]
            player_name = player_data['player_name'][i]
            balance = player_data['balance'][i]
            net = player_data['net_gain'][i]
            new_plr = Player(player_id, player_name, balance, net)
            players.append(new_plr)
        return players

    def get_debts(self):
        debt_history = self.__get_table("debt_history")
        debts = []
        for i in debt_history.index:
            debt_type = debt_history['debt_type'][i]
            recipient_id = debt_history['recipient_id'][i]
            payer_id = debt_history['payer_id'][i]
            amount = debt_history['amount'][i]
            date = debt_history['date'][i]
            new_debt = Debt(debt_type, recipient_id, payer_id, amount, date)
            debts.append(new_debt)
        return debts

    def update_player_balance(self, balance, user):
        self.__execute_query("USE cashgamebot")
        query = (f"UPDATE player_data "
                 f"SET balance = {balance} "
                 f"WHERE player_id = {user.id} AND player_name = '{user.name}'")
        row_count = self.__execute_query(query)
        if not row_count or row_count < 1:
            return False
        return balance

    def add_player_balance(self, amount, user):
        self.__execute_query("USE cashgamebot")
        current_balance = self.get_player(user).balance
        query = (f"UPDATE player_data "
                 f"SET balance = {current_balance + amount} "
                 f"WHERE player_id = {user.id} AND player_name = '{user.name}'")
        row_count = self.__execute_query(query)
        if not row_count or row_count < 1:
            return False
        return current_balance + amount

    def update_player_net(self, net, user):
        self.__execute_query("USE cashgamebot")
        query = (f"UPDATE player_data "
                 f"SET net_gain = {net} "
                 f"WHERE player_id = {user.id} AND player_name = '{user.name}'")
        row_count = self.__execute_query(query)
        if not row_count or row_count < 1:
            return False
        return net

    def add_player_net(self, amount, user):
        self.__execute_query("USE cashgamebot")
        current_net = self.get_player(user).net
        query = (f"UPDATE player_data "
                 f"SET net_gain = {current_net + amount} "
                 f"WHERE player_id = {user.id} AND player_name = '{user.name}'")
        row_count = self.__execute_query(query)
        if not row_count or row_count < 1:
            return False
        return current_net + amount

    def get_player(self, user):
        self.__execute_query("USE cashgamebot")
        query = (f"SELECT * "
                 f"FROM player_data "
                 f"WHERE player_id = {user.id} AND player_name = '{user.name}'")
        plr = pd.read_sql(query, self.connection)
        if not plr.empty:
            return Player(plr.iloc[0]['player_id'], plr.iloc[0]['player_name'], plr.iloc[0]['balance'],
                          plr.iloc[0]['net_gain'])
        return None

    def add_debt(self, debt_type, recipient_id, payer_id, amount):
        self.__execute_query("USE cashgamebot")
        query = (f"INSERT INTO debt_history (debt_type, recipient_id, payer_id, amount, date) VALUES "
                 f"('{debt_type}', {recipient_id}, {payer_id}, {amount}, "
                 f"'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')")
        self.__execute_query(query)
        self.add_player_balance(recipient_id, amount)
        self.add_player_balance(payer_id, -amount)
        if amount >= 0:
            self.add_player_net(recipient_id, amount)
            self.add_player_net(payer_id, amount)

    def add_player(self, user):
        if not self.get_player(user):
            self.__execute_query("USE cashgamebot")
            query = f"INSERT INTO player_data (player_id, player_name) VALUES ({user.id}, '{user.name}')"
            row_count = self.__execute_query(query)
            if not row_count or row_count < 1:
                return False
            return True
        return False

    def refresh_balances(self):
        debt_history = self.__get_table("debt_history")
        player_data = self.__get_table("player_data")
        for i in player_data.index:
            self.update_player_balance(i+1, 0)
        for j in debt_history.index:
            recipient_id = debt_history['recipient_id'][j]
            payer_id = debt_history['payer_id'][j]
            amount = debt_history['amount'][j]
            self.add_player_balance(recipient_id, amount)
            self.add_player_balance(payer_id, -amount)
            if amount >= 0:
                self.add_player_net(recipient_id, amount)
                self.add_player_net(payer_id, -amount)




