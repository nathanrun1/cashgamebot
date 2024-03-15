from balances import Balances
from discord.ext import commands
from bot_token import TOKEN
from table2ascii import table2ascii as t2a, PresetStyle
import numpy as np
import discord

# Make file bot_token.py, put "TOKEN = "<TOKEN>" in it, do not commit this file to repo."
BOT_TOKEN = TOKEN
CHANNEL_ID = 1217961509414764705
USAGES = {
    "add": "lb <to_add> <arg1> <arg2> ...",
    "info": "lb info <player_name>",
    "debt": "lb debt <player_name>",
    "session": "lb session [buyin|cashout] <player_name> <bank_player_name> <amount>",
}

balances = Balances()

bot = commands.Bot(command_prefix=("LB ", "lb ", "Lb ", "lB "),
                   intents=discord.Intents.all(),
                   case_insensitive=True)


def get_user(user_name=None, user_id=None):
    for user in bot.users:
        if user_name == user.name or user_id == user.id:
            return user
    return False


def get_leaderboard():
    players = balances.get_players()
    sorted_players = np.flip(np.sort(players))
    new_leaderboard = []
    current_rank = 1
    current_bal = None
    for player in sorted_players:
        if current_bal is not None and current_bal != player.balance:
            current_rank += 1
        current_bal = player.balance
        row = [str(current_rank), player.name, f"{f"${player.net}" if player.net >= 0 else
            f"-${-player.net}"}"]
        new_leaderboard.append(row)
    return new_leaderboard


def get_rank(user):
    plr_name = user.name
    current_leader_board = get_leaderboard()
    for row in current_leader_board:
        if row[1] == plr_name:
            return row[0]


@bot.event
async def on_ready():
    print("BOT IS READY")
    print(f"USERS:\n{bot.users}")
    channel = bot.get_channel(CHANNEL_ID)
    # await channel.send("message")
    get_leaderboard()


@bot.command()
async def leaderboard(ctx):
    leaderboard_ascii = t2a(
        header=["Rank", "Player", "Net Winnings"],
        body=get_leaderboard(),
        style=PresetStyle.thin_compact
    )
    await ctx.send(f"```{leaderboard_ascii}```")


@bot.command()
async def add(ctx, *args):
    if not args:
        await ctx.send(f"Usage: ```{USAGES["add"]}```")
        return
    args_amnt = len(args)
    if args[0] == "player":
        plr_name = args[1]
        user = get_user(user_name=plr_name)
        if not len(args) > 1:
            await ctx.send("Usage: ```lb add player <player_name>```")
            return
        if user:
            try:
                success = balances.add_player(user)
                if success:
                    await ctx.send(f"Player '{plr_name}' added successfully. ")
                else:
                    await ctx.send(f"Player '{plr_name}' has already been added or an error occurred")
            except Exception as e:
                await ctx.send(f"ERROR: {str(e)}")
        else:
            await ctx.send(f"User '{plr_name}' is not in the server, or you used an @")


@bot.command()
async def info(ctx, *args):
    if not args:
        await ctx.send(f"Usage: ```{USAGES["info"]}```")
        return
    plr_name = args[0]
    user = get_user(user_name=plr_name)
    embed = discord.Embed(title=f"{plr_name}", color=0x03f8fc, timestamp=ctx.message.created_at)
    if user:
        try:
            plr = balances.get_player(user)
            if plr:
                embed.add_field(name="Rank", value=f"{get_rank(user)}", inline=True)
                embed.add_field(name="Net Winnings", value=f"${plr.net}", inline=True)
                embed.add_field(name="Balance", value=f"${plr.balance}", inline=False)
                await ctx.send(embed=embed)
            else:
                await ctx.send(f"Player '{plr_name}' has not been added yet or an error occurred. "
                               f"Try adding the player: ```lb add player {plr_name}```")
        except Exception as e:
            await ctx.send(f"ERROR: {str(e)}")
    else:
        await ctx.send(f"User '{plr_name}' is not in the server, or you used an @")


@bot.command()
async def debt(ctx, *args):
    if not args:
        await ctx.send(f"Usage: ```{USAGES["debt"]}```")
        return
    plr_name = args[0]
    user = get_user(user_name=plr_name)
    if not user:
        await ctx.send(f"User '{plr_name}' is not in the server, or you used an @")
    # try:
    plr = balances.get_player(user)
    if plr:
        all_debts = balances.get_debts()
        relevant_debts = filter(lambda d: d.recipient_id == plr.player_id or d.payer_id == plr.player_id, all_debts)
        owed = {}
        for d in relevant_debts:
            if d.recipient_id == plr.player_id:
                payer = get_user(user_id=d.payer_id)
                if f"{payer.name}" in owed.keys():
                    owed[f"{payer.name}"] += d.amount
                else:
                    owed[f"{payer.name}"] = d.amount
            if d.payer_id == plr.player_id:
                recipient = get_user(user_id=d.recipient_id)
                if f"{recipient.name}" in owed.keys():
                    owed[f"{recipient.name}"] -= d.amount
                else:
                    owed[f"{recipient.name}"] = d.amount
        debt_table = t2a(
            header=["Owed To/By", "Amount"],
            body=list([k, owed[k]] for k in owed if owed[k] != 0),
            style=PresetStyle.thin_compact
        )
        if owed:
            await ctx.send(f"```{debt_table}```")
            await ctx.send(f"Positive: Owed to {plr_name}\nNegative: Owed by {plr_name}")
        else:
            await ctx.send(f"Player '{plr_name}' does not owe and is not owed.")
    else:
        await ctx.send(f"Player '{plr_name}' has not been added yet or an error occurred. "
                       f"Try adding the player: ```lb add player {plr_name}```")
    # except Exception as e:
    #     await ctx.send(f"ERROR: {str(e)}")

@bot.command()
async def session(ctx, *args):
    cmd_types = ("start", "buyin", "cashout", "end")
    if not (args and (len(args) == 4) and args[0] in cmd_types):
        await ctx.send(f"Usage: ```{USAGES["session"]}```")
        return
    cmd_type = args[0]
    payer_name = args[1]
    bank_name = args[2]
    amount = float(args[3])
    if amount <= 0:
        await ctx.send(f"Invalid Amount {amount}, must be greater than 0.")
        return
    payer = get_user(user_name=payer_name)
    bank = get_user(user_name=bank_name)
    if not payer:
        await ctx.send(f"User '{payer_name}' is not in the server, or you used an @")
        return
    if not bank:
        await ctx.send(f"User '{bank_name}' is not in the server, or you used an @")
        return
    try:
        payer_plr = balances.get_player(payer)
        bank_plr = balances.get_player(bank)
        if not payer_plr:
            await ctx.send(f"Player '{payer_name}' has not been added yet or an error occurred. "
                           f"Try adding the player: ```lb add player {payer_name}```")
            return
        if not bank_plr:
            await ctx.send(f"Player '{bank_name}' has not been added yet or an error occurred. "
                           f"Try adding the player: ```lb add player {bank_name}```")
            return
        if cmd_type == "buyin":
            balances.add_debt("buyin", bank, payer, amount)
            await ctx.send(f"Player '{payer_name}' has bought in for ${amount}. Bank: '{bank_name}'.\n"
                           f"The following debt has been added:\n"
                           f"```{payer_name} owes ${amount} to {bank_name}```")
        else:
            balances.add_debt("cashout", payer, bank, amount)
            await ctx.send(f"Player '{payer_name}' has cashed out for ${amount}. Bank: '{bank_name}'.\n"
                           f"The following debt has been added:\n"
                           f"```{bank_name} owes ${amount} to {payer_name}```")
    except Exception as e:
        await ctx.send(f"ERROR: {str(e)}")

bot.run(BOT_TOKEN)
