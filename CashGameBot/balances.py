import datetime
import pandas as pd

"""
Debt_history:
    For every element:
        Payer.balance -= Amount
        Recipient.balance += Amount 
Types:
    Payment: Payer settles debt. Recipient balance goes down, Payer balance goes up.
    Buy in & Cash out: Payer gains debt. Recipient balance goes up, Payer balance goes down.
"""


class Player:
    def __init__(self, name, id):
        self.id = id
        self.name = name
        self.balance = 0

    def __str__(self):
        return f"{self.name} (Balance: {self.balance})"

    def __eq__(self, other):
        return self.id == other.id


debt_history = pd.DataFrame(columns=['Type', 'RecipientID', 'PayerID', 'Amount', 'Date/Time'])
players = []


def update_balances():
    for player in players:
        player.balance = 0
    for row in debt_history:
        recip = row['RecipientID']
        payer = row['PayerID']
        amount = row['Amount']
        players[recip].balance += amount
        players[payer].balance -= amount


def add_row(debt_type, recipient, payer, amount):
    debt_history.loc[len(debt_history)] = [debt_type, recipient, payer, amount, datetime.datetime.now()]





