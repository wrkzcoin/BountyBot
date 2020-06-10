import discord
from discord.ext import commands
from discord.ext.commands import Bot, AutoShardedBot, when_mentioned_or, CheckFailure
from discord.utils import get

import os
import time, timeago
from datetime import datetime
from config import config
import click
import sys, traceback
import asyncio, aiohttp
# ascii table
from terminaltables import AsciiTable

import uuid, json
import re, redis

import store, addressvalidation, walletapi
from wallet import *
from generic_xmr.address_xmr import address_xmr as address_xmr

# MySQL
import pymysql, pymysqlpool
import pymysql.cursors

# ascii table
from terminaltables import AsciiTable

from typing import List, Dict

# for randomString
import random
import string

# Coin using wallet-api
ENABLE_COIN = config.Enable_Coin.split(",")
ENABLE_XMR = config.Enable_Coin_XMR.split(",")
WITHDRAW_IN_PROCESS = []
QUEUE_ADD_LIST = []

redis_pool = None
redis_conn = None
redis_expired = 600

EMOJI_HOURGLASS_NOT_DONE = "\u23F3"
EMOJI_ERROR = "\u274C"
EMOJI_OK_BOX = "\U0001F197"
EMOJI_MAINTENANCE = "\U0001F527"
EMOJI_RED_NO = "\u26D4"
EMOJI_REFRESH = "\U0001F504"
EMOJI_OK_HAND = "\U0001F44D"
EMOJI_MONEYBAG = "\U0001F4B0"
EMOJI_QUESTEXCLAIM = "\u2049"
EMOJI_ARROW_RIGHTHOOK = "\u21AA"
EMOJI_THUMB_UP = "\U0001F44D"
EMOJI_THUMB_DOWN = "\U0001F44E"

EMOJI_INFINITY = "\u267E"
EMOJI_RAISEHAND = "\U0001F64B"
EMOJI_HANDFINGERS = "\U0001F590"

EMOJI_MEMO = "\U0001F4DD"
EMOJI_GREENAPPLE = "\U0001F34F"
EMOJI_PINEAPPLE = "\U0001F34D"
EMOJI_TARGET = "\U0001F3AF"


NOTICE_START = """
```
By creating a bountry, you will need to accept the following term:\n
1) Upon bounty submitted, a non-refundable fee of """+ num_format_coin(config.charge_wrkz.creating, "WRKZ")+"""WRKZ will apply

2) Maintenance fee """+"{0:.0%}".format(config.Bounty_Fee_Margin)+""" for each bounty completed

3) Bot shall notify you in case of bounty application, submission of completion to where you created

4) Once you command a bounty as "complete", bounty taker will receive those reward and not able to refund

5) This CryptoBountyBot is still under testing

* Consider donation via donate command if you like it
```
"""

COMMAND_IN_PROGRESS = []
IS_RESTARTING = False

pymysqlpool.logger.setLevel('DEBUG')
myconfig = {
    'host': config.mysql.host,
    'user':config.mysql.user,
    'password':config.mysql.password,
    'database':config.mysql.db,
    'charset':'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit':True
    }

connPool = pymysqlpool.ConnectionPool(size=4, name='connPool', **myconfig)
conn = connPool.get_connection(timeout=5, retry_num=2)

bot_help_about = "About BountyBot."
bot_help_invite = "Invite link of bot to your server."
bot_help_donate = "Donate to support Crypto BountyBot."
bot_help_usage = "Show current stats"
bot_help_balance = "Check your bountybot balance."
bot_help_deposit = "Get your wallet ticker's deposit address."
bot_help_settings = "settings view and set for prefix, etc. Requires permission manage_channels"
bot_help_settings_prefix = "Set prefix of CryptoBountyBot in your guild."
bot_help_coininfo = "List of coin status in CryptoBountyBot."
bot_help_register = "Register or change your deposit address for CryptoBountyBot."
bot_help_withdraw = "Withdraw coin from your CryptoBountyBot balance."

bot_help_bounty = "Manage bounty."
bot_help_bounty_add = "Add or cancel a bounty."
bot_help_bounty_edit = "Edit a bounty."
bot_help_bounty_search = "Search active bounties."
bot_help_bounty_detail = "View a bounty in detail by a ref number."
bot_help_bounty_apply = "Apply for a bounty by a ref number."
bot_help_bounty_end = "End a bounty. Possible only if no one is taken yet."
bot_help_bounty_cancel = "Cancel a progressing a bounty entry."
bot_help_bounty_cancelapply = "Cancel an application for a bounty. Possible only if Bounty Owner has not accepted application."
bot_help_bounty_confirm_app = "Accept an application by an application ref number."
bot_help_bounty_mylist = "List of your active bounties."
bot_help_bounty_mylist_app = "List all your applied bounties."
bot_help_bounty_complete = "Mark a bounty as completed. Any active bounty taker will received bounty after this command."
bot_help_bounty_submit = "Submit a result of a bounty by ref number."

bot_help_admin_shutdown = "Restart bot."
bot_help_admin_maintenance = "Bot to be in maintenance mode ON / OFF"

# Steal from https://github.com/cree-py/RemixBot/blob/master/bot.py#L49
async def get_prefix(bot, message):
    """Gets the prefix for the guild"""
    pre_cmd = config.discord.prefixCmd
    if isinstance(message.channel, discord.DMChannel):
        pre_cmd = config.discord.prefixCmd
        extras = [pre_cmd, 'btb.', 'btb!', '?', '.', '+', '!', '-']
        return when_mentioned_or(*extras)(bot, message)

    serverinfo = store.sql_info_by_server(str(message.guild.id))
    if serverinfo is None:
        # Let's add some info if guild return None
        add_server_info = store.sql_addinfo_by_server(str(message.guild.id), message.guild.name,
                                                      config.discord.prefixCmd)
        pre_cmd = config.discord.prefixCmd
        serverinfo = store.sql_info_by_server(str(message.guild.id))
    if serverinfo and ('prefix' in serverinfo):
        pre_cmd = serverinfo['prefix']
    else:
        pre_cmd =  config.discord.prefixCmd
    extras = [pre_cmd, 'btb.', 'btb!']
    return when_mentioned_or(*extras)(bot, message)

bot = AutoShardedBot(command_prefix=get_prefix, owner_id = config.discord.ownerID, case_insensitive=True)


def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, db=10)


def openRedis():
    global redis_pool, redis_conn
    if redis_conn is None:
        try:
            redis_conn = redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)


# connPool 
def openConnection():
    global conn, connPool
    try:
        if conn is None:
            conn = connPool.get_connection(timeout=5, retry_num=2)
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()


@bot.event
async def on_shard_ready(shard_id):
    print(f'Shard {shard_id} connected')

@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')
    game = discord.Game(name="btb.")
    await bot.change_presence(status=discord.Status.online, activity=game)


@bot.event
async def on_guild_join(guild):
    botLogChan = bot.get_channel(id=config.discord.logChan)
    await botLogChan.send(f'Bot joins a new guild {guild.name} / {guild.id}. Total guilds: {len(bot.guilds)}.')
    return


@bot.event
async def on_guild_remove(guild):
    botLogChan = bot.get_channel(id=config.discord.logChan)
    await botLogChan.send(f'Bot was removed from guild {guild.name} / {guild.id}. Total guilds: {len(bot.guilds)}')
    return


@bot.event
async def on_message(message):
    # ignore .help in public
    if message.content.upper().startswith('.HELP') and isinstance(message.channel, discord.DMChannel) == False:
        # await message.channel.send('Help command is available via Direct Message (DM) only.')
        return
    # Do not remove this, otherwise, command not working.
    ctx = await bot.get_context(message)
    await bot.invoke(ctx)


@bot.event
async def on_reaction_add(reaction, user):
    # If bot re-act, ignore.
    if user.id == bot.user.id:
        return
    # If other people beside bot react.
    else:
        # If re-action is OK box and message author is bot itself
        if reaction.emoji == EMOJI_OK_BOX and reaction.message.author.id == bot.user.id:
            await reaction.message.delete()


@bot.event
async def on_raw_reaction_add(payload):
    if payload.guild_id is None:
        return  # Reaction is on a private message
    """Handle a reaction add."""
    try:
        emoji_partial = str(payload.emoji)
        message_id = payload.message_id
        channel_id = payload.channel_id
        user_id = payload.user_id
        guild = bot.get_guild(payload.guild_id)
        channel = bot.get_channel(id=channel_id)
        if not channel:
            return
        if isinstance(channel, discord.DMChannel):
            return
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return
    message = None
    author = None
    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            author = message.author
        except (discord.errors.NotFound, discord.errors.Forbidden) as e:
            # No message found
            return
        member = bot.get_user(id=user_id)
        if emoji_partial in [EMOJI_OK_BOX] and message.author.id == bot.user.id \
            and author != member and message:
            # Delete message
            try:
                await message.delete()
                return
            except discord.errors.NotFound as e:
                # No message found
                return


@bot.group(hidden = True)
@commands.is_owner()
async def admin(ctx):
    if ctx.invoked_subcommand is None:
        await ctx.send('Invalid `admin` command passed...')
    return


@commands.is_owner()
@admin.command(aliases=['maintenance'])
async def maint(ctx, coin: str=None):
    botLogChan = bot.get_channel(id=config.discord.logChan)
    if coin is None:
        if is_maintenance():
            await ctx.send(f'{EMOJI_OK_BOX} bot maintenance **OFF** in BountyBot.')
            set_maint = set_maintenance(False)
        else:
            await ctx.send(f'{EMOJI_OK_BOX} bot maintenance **ON** in BountyBot.')
            set_maint = set_maintenance(True)
    else:
        COIN_NAME = coin.upper()
        if COIN_NAME not in ENABLE_COIN:
            await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list in BountyBot.')
            return

        if is_maintenance_coin(COIN_NAME):
            await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** to maintenance **OFF** in BountyBot.')
            set_main = set_maintenance_coin(COIN_NAME, False)
        else:
            await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** to maintenance **ON** in BountyBot.')
            set_main = set_maintenance_coin(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(pass_context=True, name='shutdown', aliases=['restart'], help=bot_help_admin_shutdown)
async def shutdown(ctx):
    global IS_RESTARTING
    botLogChan = bot.get_channel(id=config.discord.logChan)
    if IS_RESTARTING:
        await ctx.send(f'{ctx.author.mention} I already got this command earlier.')
        return
    IS_MAINTENANCE = 1
    IS_RESTARTING = True
    await ctx.send(f'{ctx.author.mention} .. I will restarting in 30s.. back soon.')
    await botLogChan.send(f'{ctx.message.author.name}#{ctx.message.author.discriminator} called `restart`. I am restarting in 30s and will back soon hopefully.')
    await asyncio.sleep(30)
    await bot.logout()


@commands.is_owner()
@admin.command(aliases=['withdraw'])
async def withdrawable(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list in BountyBot.')
        return

    if is_withdrawable_coin(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **DISABLE** withdraw in BountyBot.')
        set_main = set_withdrawable_coin(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **ENABLE** withdraw in BountyBot.')
        set_main = set_withdrawable_coin(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(aliases=['deposit'])
async def depositable(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    if is_depositable_coin(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **DISABLE** deposit in BountyBot.')
        set_main = set_depositable_coin(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **ENABLE** deposit in BountyBot.')
        set_main = set_depositable_coin(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(aliases=['addbalance'])
async def credit(ctx, amount: str, coin: str, to_userid: str):
    global IS_RESTARTING
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    # check if bot can find user
    member = bot.get_user(id=int(to_userid))
    if not member:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} I cannot find user with userid **{to_userid}**.')
        return
    # check if user / address exist in database
    amount = amount.replace(",", "")
    try:
        amount = float(amount)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid credit amount.')
        return

    coin_family = None
    wallet = None
    try:
        coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **INVALID TICKER**')
        return

    COIN_DEC = get_decimal(COIN_NAME)
    real_amount = int(amount * COIN_DEC) if coin_family in ["XMR", "TRTL"] else amount * COIN_DEC

    if coin_family == "TRTL" or coin_family == "CCX" or coin_family == "XMR":
        wallet = await store.sql_get_userwallet(to_userid, COIN_NAME, 'DISCORD')
        if wallet is None:
            userregister = await store.sql_register_user(to_userid, COIN_NAME, 'DISCORD')
            wallet = await store.sql_get_userwallet(to_userid, COIN_NAME, 'DISCORD')
    credit_to = store.sql_cn_xmr_credit(str(ctx.message.author.id), to_userid, real_amount, COIN_NAME, ctx.message.content)
    if credit_to:
        msg = await ctx.send(f'{ctx.author.mention} amount **{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}** has been credited to userid **{to_userid}**.')
        return


@bot.command(pass_context=True, name='about', help=bot_help_about)
async def about(ctx):
    invite_link = "https://discordapp.com/oauth2/authorize?client_id="+str(bot.user.id)+"&scope=bot"
    botdetails = discord.Embed(title='About Me', description='', colour=7047495)
    botdetails.add_field(name='My Github:', value='https://github.com/wrkzcoin/BountyBot', inline=False)
    botdetails.add_field(name='Invite Me:', value=f'{invite_link}', inline=False)
    botdetails.add_field(name='Servers I am in:', value=len(bot.guilds), inline=False)
    botdetails.add_field(name='Supported by:', value='WrkzCoin Community Team', inline=False)
    botdetails.add_field(name='Supported Server:', value='https://chat.wrkz.work', inline=False)
    botdetails.set_footer(text='Made in Python3.6+ with discord.py library!', icon_url='http://findicons.com/files/icons/2804/plex/512/python.png')
    botdetails.set_author(name=bot.user.name, icon_url=bot.user.avatar_url)
    try:
        await ctx.send(embed=botdetails)
    except Exception as e:
        await ctx.message.author.send(embed=botdetails)
        traceback.print_exc(file=sys.stdout)



@bot.command(pass_context=True, name='donate', help=bot_help_donate)
async def donate(ctx):
    invite_link = "https://discordapp.com/oauth2/authorize?client_id="+str(bot.user.id)+"&scope=bot"
    donatelist = discord.Embed(title='Support Me', description='', colour=7047495)
    donatelist.add_field(name='BTC:', value=config.donate.btc, inline=False)
    donatelist.add_field(name='LTC:', value=config.donate.ltc, inline=False)
    donatelist.add_field(name='DOGE:', value=config.donate.doge, inline=False)
    donatelist.add_field(name='BCH:', value=config.donate.bch, inline=False)
    donatelist.add_field(name='DASH:', value=config.donate.dash, inline=False)
    donatelist.add_field(name='XMR:', value=config.donate.xmr, inline=False)
    donatelist.add_field(name='WRKZ:', value=config.donate.wrkz, inline=False)
    donatelist.set_author(name=bot.user.name, icon_url=bot.user.avatar_url)
    try:
        await ctx.send(embed=donatelist)
        return
    except Exception as e:
        await ctx.message.author.send(embed=donatelist)
        traceback.print_exc(file=sys.stdout)


@bot.command(pass_context=True, name='invite', aliases=['inviteme'], help=bot_help_invite)
async def invite(ctx):
    invite_link = "https://discordapp.com/oauth2/authorize?client_id="+str(bot.user.id)+"&scope=bot"
    await ctx.send('**[INVITE LINK]**\n\n'
                f'{invite_link}')


@bot.group(name='setting', aliases=['settings', 'set'], help=bot_help_settings)
@commands.has_permissions(manage_channels=True)
async def setting(ctx):
    if isinstance(ctx.channel, discord.DMChannel):
        await ctx.send('This command is not available in DM.')
        return
    if ctx.invoked_subcommand is None:
        await ctx.send('Invalid `admin` command passed...')
    return


@setting.command(help=bot_help_settings_prefix)
async def prefix(ctx, prefix: str):
    if isinstance(ctx.channel, discord.DMChannel):
        await ctx.send(f'{ctx.author.mention} This command is not available in DM.')
        return

    allow_prefix = [".", "?", "*", "!", "$", "~"]
    if prefix not in allow_prefix:
        allowed = ', '.join(allow_prefix)
        await ctx.send(f'{ctx.author.mention} Invalid prefix set. Use any of this **{allowed}**.')
        return
    else:
        serverinfo = store.sql_info_by_server(str(ctx.guild.id))
        server_prefix = config.discord.prefixCmd
        if serverinfo is None:
            # Let's add some info if server return None
            add_server_info = store.sql_addinfo_by_server(str(ctx.guild.id),
                                                        ctx.message.guild.name, config.discord.prefixCmd)
        else:
            server_prefix = serverinfo['prefix']
        if server_prefix == prefix:
            await ctx.send(f'{ctx.author.mention} That\'s the default prefix. Nothing changed.')
            return
        else:
            changeinfo = store.sql_changeinfo_by_server(str(ctx.guild.id), 'prefix', prefix)
            await ctx.send(f'{ctx.author.mention} Prefix changed from `{server_prefix}` to `{prefix}`.')
            return


@bot.group(name='bounty', aliases=['btb'], help=bot_help_bounty)
async def bounty(ctx):
    prefix = await get_guild_prefix(ctx)
    if isinstance(ctx.channel, discord.DMChannel):
        await ctx.send('If you run this command loop in DM, the bounty is for globally listed.')
    if ctx.invoked_subcommand is None:
        await ctx.send(f'{ctx.author.mention} Invalid bounty command.\n'
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty subcommand')
    return


@bounty.command(help=bot_help_bounty_add)
async def add(ctx):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    prefix = await get_guild_prefix(ctx)
    # Check if user has enough WRKZ
    COIN_NAME = "WRKZ"
    user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
    if user_from is None:
        user_from = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
    userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
    balance_having = user_from['actual_balance'] + float(userdata_balance['Adjust'])
    need_amount = int(config.charge_wrkz.creating * get_decimal(COIN_NAME))
    if need_amount > balance_having:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient {COIN_NAME} balance to register a bounty.\n'
                       f'Need:     {num_format_coin(need_amount, COIN_NAME)}{COIN_NAME}\n'
                       f'You have: {num_format_coin(balance_having, COIN_NAME)}{COIN_NAME}\n.'
                       f'Please deposit **{COIN_NAME}** via `{prefix}deposit {COIN_NAME}`.')
        return
    
    if ctx.message.author.id in QUEUE_ADD_LIST:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you are still in another progress of adding bounty.\n'
                       'You can cancel it also by: {prefix}bounty cancel')
        return

    random_string = randomString(16).upper() # use this as reference
    try:
        msg = await ctx.send(f'{ctx.author.mention} You are creating a bounty. Please input or re-act as bot requested.\n'
                             f'{NOTICE_START}'
                             f'Re-act: {EMOJI_THUMB_UP} if you agree.')
        await msg.add_reaction(EMOJI_THUMB_UP)

        def check(reaction, user):
            return user == ctx.message.author and reaction.message.author == bot.user and reaction.message.id == msg.id and str(reaction.emoji) == EMOJI_THUMB_UP
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=config.Bounty_Setting.default_timeout, check=check)
        except asyncio.TimeoutError:
            await ctx.send(f'{ctx.author.mention} too long. We assumed you are {EMOJI_THUMB_DOWN}')
            return
        else:
            await ctx.send(f'{ctx.author.mention} Thank you for {EMOJI_THUMB_UP}. Your ongoing bounty ref_id: **{random_string}**. Let\'s move to next step.')
            pass
        # store temporary data and start interactive
        QUEUE_ADD_LIST.append(ctx.message.author.id)
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":ref_id"
        redis_conn.set(key, random_string)
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":userid_create"
        redis_conn.set(key, str(ctx.message.author.id))
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":created_date"
        redis_conn.set(key, str(int(time.time())))
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":created_user_server"
        redis_conn.set(key, 'DISCORD')
        msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                             f'Please type in **amount coin_name** for the bounty price (timeout {config.Bounty_Setting.price_timeout}s):\n'
                             'Supported coin: {}'.format(", ".join(ENABLE_COIN)))

        amount = None
        COIN_NAME = None
        while (amount is None) or (COIN_NAME not in ENABLE_COIN):
            waiting_pricemsg = None
            try:
                waiting_pricemsg = await bot.wait_for('message', timeout=config.Bounty_Setting.price_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during price input.')
                return
            if waiting_pricemsg is None:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during price input.')
                return
            else:
                msg_content = waiting_pricemsg.content.split(" ")
                if len(msg_content) != 2:
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{ctx.author.mention} Please use **amount coin_name**. Example `1,000,000 WRKZ`')
                else:
                    amount = msg_content[0]
                    COIN_NAME = msg_content[1].upper()
                    if COIN_NAME not in ENABLE_COIN:
                        accepted_coin = ", ".join(ENABLE_COIN)
                        await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                        await ctx.send(f'{ctx.author.mention} Please use accepted coin: **{accepted_coin}**.')
                    else:
                        amount = amount.replace(",", "")
                        try:
                            amount = float(amount)
                            MinTx = get_min_bounty(COIN_NAME)
                            MaxTX = get_max_bounty(COIN_NAME)
                            real_amount = amount*get_decimal(COIN_NAME)
                            if MinTx <= real_amount <= MaxTX:
                                # check user balance
                                user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
                                if user_from is None:
                                    user_from = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                                    user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
                                userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                                user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])
                                if user_from['actual_balance'] < real_amount:
                                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                    await ctx.send(f'{ctx.author.mention} Insufficient balance to make a bounty of '
                                                   f'**{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}**\n'
                                                   'Try to lower the bounty amount again.')
                                    amount = None
                            else:
                                await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                await ctx.send(f'{ctx.author.mention} Amount input is not between '
                                               f'{num_format_coin(MinTx, COIN_NAME)}{COIN_NAME}, {num_format_coin(MaxTX, COIN_NAME)}{COIN_NAME}.')
                                amount = None
                        except ValueError:
                            amount = None
                            await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid amount.')
        msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                             f'Bounty amount set: **{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}**\n'
                             f'Next, please give a title of this bounty (min. {config.Bounty_Setting.title_min}, '
                             f'max. {config.Bounty_Setting.title_max} chars, timeout {config.Bounty_Setting.title_timeout}s):')
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_amount"
        redis_conn.set(key, str(real_amount))
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_amount_after_fee"
        redis_conn.set(key, str(real_amount*(1 - config.Bounty_Fee_Margin)))
        key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_coin_decimal"
        redis_conn.set(key, str(get_decimal(COIN_NAME)))
        # TITLE
        title = None
        while title is None:
            waiting_titlemsg = None
            try:
                waiting_titlemsg = await bot.wait_for('message', timeout=config.Bounty_Setting.title_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during title input.')
                return
            if waiting_titlemsg is None:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during title input.')
                return
            else:
                if config.Bounty_Setting.title_min <= len(waiting_titlemsg.content) <= config.Bounty_Setting.title_max:
                    title = waiting_titlemsg.content.strip()
                    key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_title"
                    redis_conn.set(key, title)
                    msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                         f'Bounty title set: **{title}**\n'
                                         f'Next, please give a objective of this bounty (min. {config.Bounty_Setting.obj_min}, '
                                         f'max. {config.Bounty_Setting.obj_max} chars, timeout {config.Bounty_Setting.obj_timeout}s):')
                else:
                    title = None
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                   f'{ctx.author.mention} title too long or too short.')
        # Objective
        objective = None
        while objective is None:
            waiting_objmsg = None
            objective = "N/A"
            try:
                waiting_objmsg = await bot.wait_for('message', timeout=config.Bounty_Setting.obj_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you put **{objective}** as objective.'
                               f'Next, please give a description of this bounty (min. {config.Bounty_Setting.desc_min}, '
                               f'max. {config.Bounty_Setting.desc_max} chars, timeout {config.Bounty_Setting.desc_timeout}s):')
                key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_obj"
                redis_conn.set(key, objective)
            if waiting_objmsg is None:
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you put **{objective}** as objective.'
                               f'Next, please give a description of this bounty (min. {config.Bounty_Setting.desc_min}, '
                               f'max. {config.Bounty_Setting.desc_max} chars):')
                key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_obj"
                redis_conn.set(key, objective)
            else:
                if config.Bounty_Setting.obj_min <= len(waiting_objmsg.content) <= config.Bounty_Setting.obj_max:
                    objective = waiting_objmsg.content.strip()
                    key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_obj"
                    redis_conn.set(key, objective)
                    msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                         f'Bounty objective set: **{objective}**\n'
                                         f'Next, please give a description of this bounty (min. {config.Bounty_Setting.desc_min}, '
                                         f'max. {config.Bounty_Setting.desc_max} chars):')
                else:
                    objective = None
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                   f'{ctx.author.mention} objective too long or too short.')
        # DESC
        desc = None
        while desc is None:
            waiting_descmsg = None
            try:
                waiting_descmsg = await bot.wait_for('message', timeout=config.Bounty_Setting.desc_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during description input.')
                return
            if waiting_descmsg is None:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during description input.')
                return
            else:
                if config.Bounty_Setting.desc_min <= len(waiting_descmsg.content) <= config.Bounty_Setting.desc_max:
                    desc = waiting_descmsg.content.strip()
                    key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_desc"
                    redis_conn.set(key, desc)
                    msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                         f'Bounty desc set: **{desc}**\n')
                else:
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                   f'{ctx.author.mention} desc too long or too short.')
        # Bounty Type
        # TODO
        msg = await ctx.send(f'{ctx.author.mention} Please select bounty type:\n'
                             f'{EMOJI_RAISEHAND}: SINGLE, {EMOJI_HANDFINGERS}: MULTIPLE, {EMOJI_INFINITY}: UMLIMITED')
        await msg.add_reaction(EMOJI_RAISEHAND)
        await msg.add_reaction(EMOJI_HANDFINGERS)
        await msg.add_reaction(EMOJI_INFINITY)
        bounty_type = None
        while bounty_type is None:
            def check(reaction, user):
                return user == ctx.message.author and reaction.message.id == msg.id  \
                and str(reaction.emoji) in (EMOJI_RAISEHAND, EMOJI_HANDFINGERS, EMOJI_INFINITY)
            try:
                reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)
            except asyncio.TimeoutError:
                bounty_type = 'SINGLE'
                await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                               f'Timeout. We use: **{bounty_type}**')
            else:
                if str(reaction.emoji) == EMOJI_INFINITY:
                    bounty_type = 'UNLIMITED'
                elif str(reaction.emoji) == EMOJI_HANDFINGERS:
                    bounty_type = 'MULTIPLE'
                elif str(reaction.emoji) == EMOJI_RAISEHAND:
                    bounty_type = 'SINGLE'
                key = "CryptoBountyBot:BTYID_" + str(ctx.message.author.id) + ":bounty_type"
                redis_conn.set(key, bounty_type)
                msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                     f'Bounty type: **{bounty_type}**\n')
        try:
            embed = discord.Embed(title="Bounty: {}".format(title), description="Here's your bountry before submit.", color=0x00ff00)
            embed.add_field(name="Amount", value=f'{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}', inline=True)
            embed.add_field(name="Creator", value=ctx.author.mention, inline=False)
            embed.add_field(name="Objective", value=objective, inline=False)
            embed.add_field(name="Description", value=desc, inline=False)
            embed.add_field(name="Bounty Type", value=bounty_type, inline=False)
            embed.add_field(name="Re-act", value=f'{EMOJI_THUMB_UP} to confirm, {EMOJI_THUMB_DOWN} to cancel', inline=False)
            embed.set_thumbnail(url=ctx.message.author.avatar_url)
            msg = await ctx.send(embed=embed)
            await msg.add_reaction(EMOJI_THUMB_DOWN)
            await msg.add_reaction(EMOJI_THUMB_UP)
            def check(reaction, user):
                return user == ctx.message.author and reaction.message.id == msg.id  \
                and str(reaction.emoji) in (EMOJI_THUMB_DOWN, EMOJI_THUMB_UP)
            reaction, user = await bot.wait_for('reaction_add', check=check)
            if str(reaction.emoji) == EMOJI_THUMB_UP:
                add = store.sql_bounty_add_data(COIN_NAME, random_string, real_amount, real_amount*(1 - config.Bounty_Fee_Margin), 
                                                get_decimal(COIN_NAME), str(ctx.message.author.id), title, objective, 
                                                desc, bounty_type, 'OPENED', 'DISCORD')
                if add: 
                    msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                         f'Status: **Submitted**')
                    delete_queue_going(ctx.message.author.id)
                    await msg.add_reaction(EMOJI_OK_BOX)
                else:
                    msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                         f'Status: **Not submitted** (Internal Error)')
                    await msg.add_reaction(EMOJI_OK_BOX)
            elif str(reaction.emoji) == EMOJI_THUMB_DOWN:
                delete_queue_going(ctx.message.author.id)
                msg = await ctx.send(f'BOUNTY ID: **{random_string}**\n'
                                     f'Status: **Cancelled**')
        except:
            error = discord.Embed(title=":exclamation: Error", description=" :warning: Failed to display!", color=0xe51e1e)
            await ctx.send(embed=error)
    except (discord.Forbidden, discord.errors.Forbidden) as e:
        # no permission to text or to re-act
        await ctx.message.add_reaction(EMOJI_ERROR)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def delete_queue_going(userid: int):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    # Delete redis and remove user from QUEUE
    if userid in QUEUE_ADD_LIST:
        QUEUE_ADD_LIST.remove(userid)
    keys = redis_conn.keys("CryptoBountyBot:BTYID_" + str(userid))
    if len(keys) > 1:
        for key in keys:
            redis_conn.delete(key)


@bounty.command(help=bot_help_bounty_edit)
async def edit(ctx, ref: str = None):
    # Remove if you are in any queue
    if ctx.message.author.id in QUEUE_ADD_LIST:
        delete_queue_going(ctx.message.author.id)
    if ref is None:
        list_bounty = store.sql_bounty_list_of_user(str(ctx.message.author.id), 'OPENED', 'DISCORD', 10)
        if list_bounty and len(list_bounty) > 0:
            # TODO: list ref number for user to select
            lists = ", ".join([item['ref_id'] for item in list_bounty])
            await ctx.send(f'{ctx.author.mention} Please use edit command with one of this ref: **{lists}**.')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you do not have any opened bounty to edit.')
            return
    else:
        ref = ref.upper()
        get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
        if get_bounty_ref:
            if get_bounty_ref['status'] != "OPENED":
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bounty **{ref}** is not **OPENED** status.')
                return
            else:
                if int(get_bounty_ref['userid_create']) != ctx.message.author.id:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bounty **{ref}** is not yours.')
                    return
                try:
                    embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
                    embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
                    embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
                    embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
                    embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
                    embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
                    embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
                    embed.add_field(name="Re-act to edit", value=f'{EMOJI_GREENAPPLE}: Title, {EMOJI_TARGET}: Objective, '
                                                                 f'{EMOJI_PINEAPPLE}: Description, \n{EMOJI_MONEYBAG}: Amount, '
                                                                 f'{EMOJI_THUMB_DOWN}: to cancel', inline=True)
                    embed.set_thumbnail(url=ctx.message.author.avatar_url)
                    msg = await ctx.send(embed=embed)
                    await msg.add_reaction(EMOJI_GREENAPPLE)
                    await msg.add_reaction(EMOJI_TARGET)
                    await msg.add_reaction(EMOJI_PINEAPPLE)
                    await msg.add_reaction(EMOJI_MONEYBAG)
                    await msg.add_reaction(EMOJI_THUMB_DOWN)
                    def check(reaction, user):
                        return user == ctx.message.author and reaction.message.id == msg.id  \
                        and str(reaction.emoji) in (EMOJI_GREENAPPLE, EMOJI_TARGET, EMOJI_PINEAPPLE, EMOJI_MONEYBAG, EMOJI_THUMB_DOWN)
                    reaction, user = await bot.wait_for('reaction_add', check=check)
                    # Edit mode
                    if str(reaction.emoji) == EMOJI_GREENAPPLE:
                        # TITLE
                        msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                             f'Please give an update title of this bounty (min. {config.Bounty_Setting.title_min}, '
                                             f'max. {config.Bounty_Setting.title_max} chars, timeout {config.Bounty_Setting.title_timeout}s):')
                        title = None
                        while title is None:
                            waiting_titlemsg = None
                            try:
                                waiting_titlemsg = await bot.wait_for('message', timeout=config.Bounty_Setting.title_timeout, check=lambda msg: msg.author == ctx.author)
                            except asyncio.TimeoutError:
                                # Delete redis and remove user from QUEUE
                                delete_queue_going(ctx.message.author.id)
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during title update.')
                                return
                            if waiting_titlemsg is None:
                                # Delete redis and remove user from QUEUE
                                delete_queue_going(ctx.message.author.id)
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during title update.')
                                return
                            else:
                                if config.Bounty_Setting.title_min <= len(waiting_titlemsg.content) <= config.Bounty_Setting.title_max:
                                    title = waiting_titlemsg.content.strip()
                                    updated = store.sql_bounty_update_by_ref(ref, 'bounty_title', title, 'DISCORD')
                                    msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                         f'Bounty title updated to: **{title}**\n')
                                    await msg.add_reaction(EMOJI_OK_BOX)
                                    # Send updated one
                                    try:
                                        get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
                                        embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
                                        embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
                                        embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
                                        embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
                                        embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
                                        embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
                                        embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
                                        embed.set_thumbnail(url=ctx.message.author.avatar_url)
                                        msg = await ctx.send(embed=embed)
                                        await msg.add_reaction(EMOJI_OK_BOX)
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                else:
                                    title = None
                                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                    await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                   f'{ctx.author.mention} title too long or too short.')
                        return
                    elif str(reaction.emoji) == EMOJI_TARGET:
                        # Objective
                        msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                             f'Please give an update objective of this bounty (min. {config.Bounty_Setting.obj_min}, '
                                             f'max. {config.Bounty_Setting.obj_max} chars, timeout {config.Bounty_Setting.obj_timeout}s):')
                        objective = None
                        while objective is None:
                            waiting_objmsg = None
                            try:
                                waiting_objmsg = await bot.wait_for('message', timeout=config.Bounty_Setting.obj_timeout, check=lambda msg: msg.author == ctx.author)
                            except asyncio.TimeoutError:
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during objective update.')
                            if waiting_objmsg is None:
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during objective update.')
                            else:
                                if config.Bounty_Setting.obj_min <= len(waiting_objmsg.content) <= config.Bounty_Setting.obj_max:
                                    objective = waiting_objmsg.content.strip()
                                    updated = store.sql_bounty_update_by_ref(ref, 'bounty_obj', objective, 'DISCORD')
                                    msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                         f'Bounty objective updated to: **{objective}**\n')
                                    await msg.add_reaction(EMOJI_OK_BOX)
                                    # Send updated one
                                    try:
                                        get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
                                        embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
                                        embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
                                        embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
                                        embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
                                        embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
                                        embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
                                        embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
                                        embed.set_thumbnail(url=ctx.message.author.avatar_url)
                                        msg = await ctx.send(embed=embed)
                                        await msg.add_reaction(EMOJI_OK_BOX)
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                else:
                                    objective = None
                                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                    await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                   f'{ctx.author.mention} objective too long or too short.')
                        return
                    elif str(reaction.emoji) == EMOJI_PINEAPPLE:
                        # Description
                        msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                             f'Please give an update description of this bounty (min. {config.Bounty_Setting.desc_min}, '
                                             f'max. {config.Bounty_Setting.desc_max} chars, timeout {config.Bounty_Setting.desc_timeout}s):')
                        desc = None
                        while desc is None:
                            waiting_descmsg = None
                            try:
                                waiting_descmsg = await bot.wait_for('message', timeout=config.Bounty_Setting.desc_timeout, check=lambda msg: msg.author == ctx.author)
                            except asyncio.TimeoutError:
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during description update.')
                            if waiting_descmsg is None:
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during description update.')
                            else:
                                if config.Bounty_Setting.desc_min <= len(waiting_descmsg.content) <= config.Bounty_Setting.desc_max:
                                    desc = waiting_descmsg.content.strip()
                                    updated = store.sql_bounty_update_by_ref(ref, 'bounty_desc', desc, 'DISCORD')
                                    msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                         f'Bounty description updated to: **{desc}**\n')
                                    await msg.add_reaction(EMOJI_OK_BOX)
                                    # Send updated one
                                    try:
                                        get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
                                        embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
                                        embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
                                        embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
                                        embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
                                        embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
                                        embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
                                        embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
                                        embed.set_thumbnail(url=ctx.message.author.avatar_url)
                                        msg = await ctx.send(embed=embed)
                                        await msg.add_reaction(EMOJI_OK_BOX)
                                    except Exception as e:
                                        traceback.print_exc(file=sys.stdout)
                                else:
                                    desc = None
                                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                    await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                   f'{ctx.author.mention} description too long or too short.')
                        return
                    elif str(reaction.emoji) == EMOJI_MONEYBAG:
                        # Amount
                        msg = await ctx.send('BOUNTY ID: **{}**\n'
                                             'Please type in **amount coin_name** to update this bounty price (timeout {}s):\n'
                                             'Supported coin: {}'.format(ref, config.Bounty_Setting.price_timeout, ", ".join(ENABLE_COIN)))
                        amount = None
                        COIN_NAME = None
                        while (amount is None) or (COIN_NAME not in ENABLE_COIN):
                            waiting_pricemsg = None
                            try:
                                waiting_pricemsg = await bot.wait_for('message', timeout=config.Bounty_Setting.price_timeout, check=lambda msg: msg.author == ctx.author)
                            except asyncio.TimeoutError:
                                # Delete redis and remove user from QUEUE
                                delete_queue_going(ctx.message.author.id)
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during bounty price update.')
                                return
                            if waiting_pricemsg is None:
                                # Delete redis and remove user from QUEUE
                                delete_queue_going(ctx.message.author.id)
                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                               f'{ctx.author.mention} too long. We assumed you are gave up during bounty price update.')
                                return
                            else:
                                msg_content = waiting_pricemsg.content.split(" ")
                                if len(msg_content) != 2:
                                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                    await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                   f'{ctx.author.mention} Please use **amount coin_name**. Example `1,000,000 WRKZ`')
                                else:
                                    amount = msg_content[0]
                                    COIN_NAME = msg_content[1].upper()
                                    if COIN_NAME not in ENABLE_COIN:
                                        accepted_coin = ", ".join(ENABLE_COIN)
                                        await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                        await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                       f'{ctx.author.mention} Please use accepted coin: **{accepted_coin}**.')
                                    else:
                                        amount = amount.replace(",", "")
                                        try:
                                            amount = float(amount)
                                            MinTx = get_min_bounty(COIN_NAME)
                                            MaxTX = get_max_bounty(COIN_NAME)
                                            real_amount = amount*get_decimal(COIN_NAME)
                                            if MinTx <= real_amount <= MaxTX:
                                                # check user balance
                                                user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
                                                if user_from is None:
                                                    user_from = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                                                    user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
                                                userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                                                user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])
                                                if user_from['actual_balance'] < real_amount:
                                                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                                    await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                                   f'{ctx.author.mention} Insufficient balance to update a bounty to '
                                                                   f'**{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}**\n'
                                                                   'Try to lower the bounty amount again.')
                                                    amount = None
                                                else:
                                                    update = store.sql_bounty_update_by_ref(ref, 'amount', 
                                                                                        json.dumps({'coin_name': COIN_NAME, 'bounty_amount': real_amount, 'bounty_amount_after_fee': real_amount,
                                                                                        'bounty_coin_decimal': get_decimal(COIN_NAME)}), 'DISCORD')
                                                    msg = await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                                         f'Bounty amount updated to: **{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}**\n')
                                                    await msg.add_reaction(EMOJI_OK_BOX)
                                                    # Send updated one
                                                    try:
                                                        get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
                                                        embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
                                                        embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
                                                        embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
                                                        embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
                                                        embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
                                                        embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
                                                        embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
                                                        embed.set_thumbnail(url=ctx.message.author.avatar_url)
                                                        msg = await ctx.send(embed=embed)
                                                        await msg.add_reaction(EMOJI_OK_BOX)
                                                    except Exception as e:
                                                        traceback.print_exc(file=sys.stdout)
                                            else:
                                                await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                                await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                               f'{ctx.author.mention} Amount input is not between '
                                                               f'{num_format_coin(MinTx, COIN_NAME)}{COIN_NAME}, {num_format_coin(MaxTX, COIN_NAME)}{COIN_NAME}.')
                                                amount = None
                                        except ValueError:
                                            amount = None
                                            await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                            await ctx.send(f'BOUNTY ID: **{ref}**\n'
                                                           f'{EMOJI_RED_NO} {ctx.author.mention} Invalid amount.')
                        return
                    elif str(reaction.emoji) == EMOJI_THUMB_DOWN:
                        delete_queue_going(ctx.message.author.id)
                        msg = await ctx.send('BOUNTY ID: **{}**\n'
                                             'Editing: **Cancelled**'.format(ref))
                        return
                except:
                    error = discord.Embed(title=":exclamation: Error", description=" :warning: Failed to display!", color=0xe51e1e)
                    await ctx.send(embed=error)
                    return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find **{ref}** in active bounty.')
            return


@bounty.command(help=bot_help_bounty_cancel)
async def cancel(ctx, ref: str = None):
    global QUEUE_ADD_LIST
    if ref is None:
        delete_queue_going(ctx.message.author.id)
        if ctx.message.author.id not in QUEUE_ADD_LIST:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you are not in any progress of adding bounty.')
            return
        else:
            QUEUE_ADD_LIST.remove(ctx.message.author.id)
            await ctx.send(f'{ctx.author.mention} Successfully cancelled progressing bounty entry.')
            return
    else:
        # find ref
        return


@bounty.command(help=bot_help_bounty_end)
async def end(ctx, ref: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    ref = ref.upper()
    get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
    if get_bounty_ref:
        if int(get_bounty_ref['userid_create']) != ctx.message.author.id:
            # Not his
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bounty **{ref}** is not yours.')
            return
        else:
            # OK his bounty
            if get_bounty_ref['status'] != 'OPENED':
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bounty **{ref}** is not **OPENED**.')
                return
            else:
                new_status = 'CANCELLED'
                update = store.sql_bounty_update_by_ref(ref, 'status', new_status, 'DISCORD')
                if update:
                    await ctx.message.add_reaction(EMOJI_OK_HAND)
                    msg = await ctx.send(f'{ctx.author.mention} Bounty **{ref}** status changed from **OPENED** to **{new_status}**.')
                    await msg.add_reaction(EMOJI_OK_BOX)
                    return
                else:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find **{ref}** bounty.')
        return
    return


@bounty.command(help=bot_help_bounty_complete)
async def complete(ctx, ref: str):
    return


@bounty.command(help=bot_help_bounty_confirm_app)
async def confirm_app(ctx, app_ref: str):
    return


@bounty.command(help=bot_help_bounty_search)
async def search(ctx, *, message):
    return

@bounty.command(help=bot_help_bounty_mylist)
async def mylist(ctx, status: str='ALL'):
    list_bounty = None
    status = status.upper()
    if status.upper() not in ['COMPLETED','OPENED','ONGOING','CANCELLED', 'ALL']:
        status_list = ", ".join(['COMPLETED','OPENED','ONGOING','CANCELLED'])
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Please give filter of correct status ({status_list}).')
        return
    else:
        list_bounty = store.sql_bounty_list_of_user(str(ctx.message.author.id), status, 'DISCORD', 20)
    if list_bounty and len(list_bounty) > 0:
        table_data = [
            ['REF', 'Amount', 'Status', 'Title']
        ]
        for each in list_bounty:
            table_data.append([each['ref_id'], num_format_coin(each['bounty_amount'], each['coin_name'])+each['coin_name'], 
                               each['status'], each['bounty_title'][:32]])
        table = AsciiTable(table_data)
        # table.inner_column_border = False
        # table.outer_border = False
        table.padding_left = 1
        table.padding_right = 1
        msg = await ctx.send('**[ YOUR BOUNTY LIST ]**\n'
                             f'```{table.table}```')
        return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you do not have any bounty.')
        return


@bounty.command(help=bot_help_bounty_mylist_app)
async def mylist_apply(ctx, status: str='ALL'):
    list_applied_bounty = None
    status = status.upper()
    if status.upper() not in ['ACCEPTED','REJECTED','APPLIED','COMPLETED','CANCELLED', 'ALL']:
        status_list = ", ".join(['ACCEPTED','REJECTED','APPLIED','COMPLETED','CANCELLED'])
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Please give filter of correct status ({status_list}).')
        return
    else:
        list_applied_bounty = store.sql_bounty_list_of_apply(str(ctx.message.author.id), status, 'DISCORD', 20)
    if list_applied_bounty and len(list_applied_bounty) > 0:
        table_data = [
            ['REF', 'Applied ID', 'Status', 'Amount']
        ]
        for each in list_applied_bounty:
            table_data.append([each['bounty_ref_id'], each['applied_id'], each['status'], 
                               num_format_coin(each['bounty_amount'], each['coin_name'])+each['coin_name']])
        table = AsciiTable(table_data)
        # table.inner_column_border = False
        # table.outer_border = False
        table.padding_left = 1
        table.padding_right = 1
        msg = await ctx.send('**[ YOUR APPLIED LIST ]**\n'
                             f'```{table.table}```')
        return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you do not have any application for a bounty.')
        return


@bounty.command(help=bot_help_bounty_detail)
async def detail(ctx, ref: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    ref = ref.upper()
    get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
    if get_bounty_ref:
        try:
            embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
            embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
            embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
            embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
            embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
            embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
            try:
                get_applicants = store.sql_bounty_get_apply_by_ref('ALL', ref, 'DISCORD')
                if get_applicants and len(get_applicants) > 0:
                    embed.add_field(name="Total applied", value=len(get_applicants), inline=False)
                    list_applied = [item['applied_id'] for item in get_applicants if item['status'] == 'APPLIED']
                    embed.add_field(name="Applied Ref", value='{}'.format(", ".join(list_applied) if len(list_applied) > 0 else 'N/A'), inline=False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
            embed.set_thumbnail(url=bot.user.avatar_url)
            msg = await ctx.send(embed=embed)
            await msg.add_reaction(EMOJI_OK_BOX)
        except:
            error = discord.Embed(title=":exclamation: Error", description=" :warning: Failed to display!", color=0xe51e1e)
            await ctx.send(embed=error)
            return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find **{ref}** bounty.')
        return
    return


@bounty.command(help=bot_help_bounty_apply)
async def apply(ctx, ref: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    ref = ref.upper()
    get_bounty_ref = store.sql_bounty_get_ref(ref, 'DISCORD')
    if get_bounty_ref:
        if int(get_bounty_ref['userid_create']) == ctx.message.author.id:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You can not apply for your own bounty **{ref}**.')
            return
        if get_bounty_ref['status'] != 'OPENED':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bounty **{ref}** is not **OPENED**.')
            return
        # Check if he already applied:
        check_apply = store.sql_bounty_get_apply_by_ref(str(ctx.message.author.id), ref, 'DISCORD')
        if check_apply:
            # Tell him he already applied and status
            status_apply = check_apply['status']
            if status_apply == 'CANCELLED':
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You already cancelled for bounty **{ref}** and status is **{status_apply}**.')
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You already applied for bounty **{ref}** and status is **{status_apply}**.')
                return
        # Check if too many people apply already
        check_apply = store.sql_bounty_get_apply_by_ref('ALL', ref, 'DISCORD')
        if check_apply and len(check_apply) > config.Max_Apply_Per_One_Bounty:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} There are many people applied for **{ref}**. Application rejected.')
            return
        try:
            embed = discord.Embed(title="Bounty: {}".format(get_bounty_ref['bounty_title']), description="REF: {}".format(ref), color=0x00ff00)
            embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_bounty_ref['bounty_amount'], get_bounty_ref['coin_name']), get_bounty_ref['coin_name']), inline=True)
            embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_bounty_ref['userid_create'], get_bounty_ref['userid_create']), inline=False)
            embed.add_field(name="Objective", value=get_bounty_ref['bounty_obj'], inline=False)
            embed.add_field(name="Description", value=get_bounty_ref['bounty_desc'], inline=False)
            embed.add_field(name="Bounty Type", value=get_bounty_ref['bounty_type'], inline=False)
            embed.add_field(name="Status", value=get_bounty_ref['status'], inline=False)
            embed.add_field(name="Re-act", value=f'{EMOJI_THUMB_UP} to apply, {EMOJI_THUMB_DOWN} to cancel', inline=False)
            embed.set_thumbnail(url=bot.user.avatar_url)
            msg = await ctx.send(embed=embed)
            await msg.add_reaction(EMOJI_THUMB_DOWN)
            await msg.add_reaction(EMOJI_THUMB_UP)
            def check(reaction, user):
                return user == ctx.message.author and reaction.message.author == bot.user and reaction.message.id == msg.id \
                and str(reaction.emoji) in (EMOJI_THUMB_UP, EMOJI_THUMB_DOWN)
            try:
                reaction, user = await bot.wait_for('reaction_add', timeout=config.Bounty_Setting.default_timeout, check=check)
                if str(reaction.emoji) == EMOJI_THUMB_UP:
                    # he want to apply
                    user_numb_bounty = store.sql_bounty_list_of_user_apply(str(ctx.message.author.id), 'ALL', 'DISCORD', 'APPLIED')
                    if user_numb_bounty and len(user_numb_bounty) > config.Max_Apply_Bounty:
                        await ctx.send(f'{ctx.author.mention} You had applied already {len(user_numb_bounty)}. Cancel some if you want to apply this **{ref}**.')
                        return
                    else:
                        # OK, let him apply
                        random_string = randomString(16).upper() 
                        add = store.sql_bounty_add_apply(get_bounty_ref['coin_name'], random_string, str(ctx.message.author.id), ref, get_bounty_ref['userid_create'], 
                                                        get_bounty_ref['bounty_amount'], get_bounty_ref['bounty_amount_after_fee'], 
                                                        get_bounty_ref['bounty_coin_decimal'], 'APPLIED', 'DISCORD')
                        if add: 
                            msg = await ctx.send(f'You applied for BOUNTY ID: **{ref}**\n'
                                                 f'Applied ID: **{random_string}**\n'
                                                 f'Status: **Applied**')
                            await msg.add_reaction(EMOJI_OK_BOX)
                        else:
                            msg = await ctx.send(f'You applied for BOUNTY ID: **{ref}**\n'
                                                 f'Status: **Not submitted** (Internal Error)')
                            await msg.add_reaction(EMOJI_OK_BOX)
                else:
                    await ctx.send(f'{ctx.author.mention} You cancelled to apply bounty: **{ref}**')
                    return
            except asyncio.TimeoutError:
                await ctx.send(f'{ctx.author.mention} too long. We assumed you are {EMOJI_THUMB_DOWN} and cancelled bounty application for **{ref}**')
                return
        except:
            error = discord.Embed(title=":exclamation: Error", description=" :warning: Failed to display!", color=0xe51e1e)
            await ctx.send(embed=error)
            return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find **{ref}** bounty.')
        return
    return


@bounty.command(help=bot_help_bounty_cancelapply)
async def cancel_apply(ctx, ref: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    ref = ref.upper()
    get_bounty_app_ref = store.sql_bounty_get_apply_by_ref(str(ctx.message.author.id), ref, 'DISCORD')
    if get_bounty_app_ref is None:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You haven\'t applied for **{ref}**.')
        return
    else:
        status_app = get_bounty_app_ref['status']
        if get_bounty_app_ref['status'] != 'APPLIED':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} can not cancelled **{ref}** which is currently **{status_app}**.')
            return
        else:
            # OK update status to cancelled
            update = store.sql_bounty_update_apply_by_ref(ref, str(ctx.message.author.id), 'status', 'CANCELLED', 'DISCORD')
            if update:
                await ctx.message.add_reaction(EMOJI_OK_HAND)
                msg = await ctx.send(f'{ctx.author.mention} You cancelled a bounty **{ref}** status changed from **{status_app}** to **CANCELLED**.')
                await msg.add_reaction(EMOJI_OK_BOX)
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                return
    return


@bounty.command(help=bot_help_bounty_submit)
async def submit(ctx, ref: str):
    return

@bot.command(pass_context=True, name='bountybot', help=bot_help_usage)
async def bountybot(ctx):
    await bot.wait_until_ready()
    get_all_m = bot.get_all_members()
    embed = discord.Embed(title="[ CryptoBounty Bot ]", description="Bot Stats", color=0xDEADBF)
    embed.set_author(name=bot.user.name, icon_url=bot.user.avatar_url)
    embed.add_field(name="Bot ID", value=str(bot.user.id), inline=False)
    embed.add_field(name="Guilds", value='{:,.0f}'.format(len(bot.guilds)), inline=False)
    embed.add_field(name="Total User Online", value='{:,.0f}'.format(sum(1 for m in get_all_m if str(m.status) != 'offline')), inline=False)
    try:
        await ctx.send(embed=embed)
        return
    except Exception as e:
        await ctx.message.author.send(embed=embed)
        traceback.print_exc(file=sys.stdout)
    return


@bot.command(pass_context=True, name='coininfo', aliases=['coinf_info', 'coin'], help=bot_help_coininfo)
async def coininfo(ctx, coin: str = None):
    if coin is None:
        table_data = [
            ["TICKER", "Height", "Withdraw", "Depth"]
            ]
        for COIN_NAME in [coinItem.upper() for coinItem in ENABLE_COIN]:
            height = None
            try:
                openRedis()
                if redis_conn and redis_conn.exists(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}'):
                    height = int(redis_conn.get(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}'))
                    if not is_maintenance_coin(COIN_NAME):
                        table_data.append([COIN_NAME,  '{:,.0f}'.format(height)
                        , "ON" if is_withdrawable_coin(COIN_NAME) else "OFF"\
                        , get_confirm_depth(COIN_NAME)])
                    else:
                        table_data.append([COIN_NAME, "***", "***", "***", get_confirm_depth(COIN_NAME)])
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
        table = AsciiTable(table_data)
        table.padding_left = 0
        table.padding_right = 0
        msg = await ctx.send('**[ BOUNTYBOT COIN LIST ]**\n'
                             f'```{table.table}```')
        await msg.add_reaction(EMOJI_OK_BOX)
        return
    else:
        COIN_NAME = coin.upper()
        if COIN_NAME not in ENABLE_COIN+ENABLE_COIN_DOGE+ENABLE_XMR:
            await ctx.send(f'{ctx.author.mention} **{COIN_NAME}** is not in our list.')
            return
        else:
            response_text = "**[ COIN INFO {} ]**".format(COIN_NAME)
            response_text += "```"
            try:
                openRedis()
                if redis_conn and redis_conn.exists(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}'):
                    height = int(redis_conn.get(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}'))
                    response_text += "Height: {:,.0f}".format(height) + "\n"
                response_text += "Confirmation: {} Blocks".format(get_confirm_depth(COIN_NAME)) + "\n"
                if is_coin_depositable(COIN_NAME): 
                    response_text += "Deposit: ON\n"
                else:
                    response_text += "Deposit: OFF\n"
                if is_withdrawable_coin(COIN_NAME): 
                    response_text += "Withdraw: ON\n"
                else:
                    response_text += "Withdraw: OFF\n"
                if COIN_NAME in FEE_PER_BYTE_COIN + ENABLE_COIN_DOGE:
                    response_text += "Reserved Fee: {}{}\n".format(num_format_coin(get_reserved_fee(COIN_NAME), COIN_NAME), COIN_NAME)
                elif COIN_NAME in ENABLE_XMR:
                    response_text += "Tx Fee: Dynamic\n"
                else:
                    response_text += "Tx Fee: {}{}\n".format(num_format_coin(get_tx_fee(COIN_NAME), COIN_NAME), COIN_NAME)
                get_tip_min_max = "Tip Min/Max:\n   " + num_format_coin(get_min_mv_amount(COIN_NAME), COIN_NAME) + " / " + num_format_coin(get_max_mv_amount(COIN_NAME), COIN_NAME) + COIN_NAME
                response_text += get_tip_min_max + "\n"
                get_tx_min_max = "Withdraw Min/Max:\n   " + num_format_coin(get_min_tx_amount(COIN_NAME), COIN_NAME) + " / " + num_format_coin(get_max_tx_amount(COIN_NAME), COIN_NAME) + COIN_NAME
                response_text += get_tx_min_max
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            response_text += "```"
            await ctx.send(response_text)
            return


@bot.command(pass_context=True, name='balance', aliases=['bal'], help=bot_help_balance)
async def balance(ctx, pub: str = None):
    global IS_RESTARTING, ENABLE_COIN
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    show_pub = False
    if pub and (pub.upper() == "PUB" or pub.upper() == "PUBLIC"):
        show_pub = True

    table_data = [
        ['TICKER', 'Available', 'On Bounty']
        ]
    for COIN_NAME in [coinItem.upper() for coinItem in ENABLE_COIN]:
        if not is_maintenance_coin(COIN_NAME):
            wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            if wallet is None:
                userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            if wallet:
                userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                wallet['actual_balance'] = int(wallet['actual_balance']) if COIN_NAME not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else wallet['actual_balance']
                balance_actual = num_format_coin(wallet['actual_balance'] + float(userdata_balance['Adjust']), COIN_NAME)
                table_data.append([COIN_NAME, balance_actual, num_format_coin(userdata_balance['OnBounty'], COIN_NAME)])
        else:
            table_data.append([COIN_NAME, "***", "***"])

    table = AsciiTable(table_data)
    table.padding_left = 0
    table.padding_right = 0
    await ctx.message.add_reaction(EMOJI_OK_HAND)
    if show_pub:
        msg = await ctx.send('**[ YOUR BALANCE LIST ]**\n'
                             f'```{table.table}```')
        await msg.add_reaction(EMOJI_OK_BOX)
    else:
        msg = await ctx.message.author.send('**[ YOUR BALANCE LIST ]**\n'
                                            f'```{table.table}```')
        await msg.add_reaction(EMOJI_OK_BOX)
    return


@bot.command(pass_context=True, name='deposit', help=bot_help_deposit)
async def deposit(ctx, coin: str, pub: str = None):
    global ENABLE_COIN

    show_pub = False
    if pub and (pub.upper() == "PUB" or pub.upper() == "PUBLIC"):
        show_pub = True
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.message.add_reaction(EMOJI_RED_NO)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} in not in our list.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} in not in our list.')
        return
    if is_maintenance_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} in maintenance.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} in maintenance.')
        return

    if not is_depositable_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} deposit currently disable.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} deposit currently disable.')
        return

    coin_family = None
    wallet = None
    try:
        coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **INVALID TICKER**')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} **INVALID TICKER**')
        return
    if coin_family in ["TRTL", "XMR", "DOGE"]:
        wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        if wallet is None:
            userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
    if wallet is None:
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Internal Error for `.info`')
        return
    if wallet['balance_wallet_address']:
        await ctx.message.add_reaction(EMOJI_OK_HAND)
        withdraw_addr = ''
        if 'user_wallet_address' in wallet and wallet['user_wallet_address']:
            withdraw_addr = "\n" + EMOJI_MONEYBAG + " Withdraw address: `" + wallet['user_wallet_address'] + "`"
        if show_pub:
            await ctx.send(f'**[{COIN_NAME} DEPOSIT INFO]**\n'
                                        f'{EMOJI_MONEYBAG} Deposit Address: `' + wallet['balance_wallet_address'] + '`'
                                        f'{withdraw_addr}')
        else:
            await ctx.message.author.send(f'**[{COIN_NAME} DEPOSIT INFO]**\n'
                                        f'{EMOJI_MONEYBAG} Deposit Address: `' + wallet['balance_wallet_address'] + '`'
                                        f'{withdraw_addr}')

    return


@bot.command(pass_context=True, name='register', aliases=['reg'], help=bot_help_register)
async def register(ctx, wallet_address: str):
    global IS_RESTARTING
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    if wallet_address.isalnum() == False:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                       f'`{wallet_address}`')
        return

    COIN_NAME = get_cn_coin_from_address(wallet_address)
    if COIN_NAME:
        pass
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Unknown Ticker.')
        return

    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")

    if is_maintenance_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {COIN_NAME} in maintenance.')
        return

    if coin_family == "TRTL" or coin_family == "XMR":
        main_address = getattr(getattr(config,"coin"+COIN_NAME),"MainAddress")
        if wallet_address == main_address:
            await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {COIN_NAME} do not register with main address. You could lose your coin when withdraw.')
            return

    user = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
    if user is None:
        userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        user = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)

    existing_user = user

    valid_address = None
    if COIN_NAME in ["BTC", "LTC", "DOGE"]:
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
        if user_from is None:
            user_from = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
        user_from['address'] = user_from['balance_wallet_address']

        valid_address = await doge_validaddress(str(wallet_address), COIN_NAME)
        if ('isvalid' in valid_address):
            if str(valid_address['isvalid']) == "True":
                valid_address = wallet_address
            else:
                valid_address = None
            pass
        pass
    elif COIN_NAME in ["WRKZ", "DEGO", "TRTL"]:
        valid_address = addressvalidation.validate_address_cn(wallet_address, COIN_NAME)
    elif COIN_NAME in ["XMR"]:
        valid_address = await validate_address_xmr(str(wallet_address), COIN_NAME)
        if valid_address is None:
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                           f'`{wallet_address}`')
        if valid_address['valid'] == True and valid_address['integrated'] == False \
        and valid_address['subaddress'] == False and valid_address['nettype'] == 'mainnet':
            # re-value valid_address
            valid_address = str(wallet_address)
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Please use {COIN_NAME} main address.')
            return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Unknown Ticker.')
        return
    # correct print(valid_address)
    if valid_address is None:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid {COIN_NAME} address:\n'
                       f'`{wallet_address}`')
        return

    if valid_address != wallet_address:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid {COIN_NAME} address:\n'
                       f'`{wallet_address}`')
        return

    # if they want to register with tipjar address
    try:
        if user['balance_wallet_address'] == wallet_address:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You can not register with your {COIN_NAME} tipjar\'s address.\n'
                           f'`{wallet_address}`')
            return
        else:
            pass
    except Exception as e:
        await ctx.message.add_reaction(EMOJI_ERROR)
        print('Error during register user address:' + str(e))
        return

    server_prefix = await get_guild_prefix(ctx)
    if 'user_wallet_address' in existing_user and existing_user['user_wallet_address']:
        prev_address = existing_user['user_wallet_address']
        if prev_address != valid_address:
            await store.sql_update_user(str(ctx.message.author.id), wallet_address, COIN_NAME)
            await ctx.message.add_reaction(EMOJI_OK_HAND)
            await ctx.send(f'Your {COIN_NAME} {ctx.author.mention} withdraw address has changed from:\n'
                           f'`{prev_address}`\n to\n '
                           f'`{wallet_address}`')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{ctx.author.mention} Your {COIN_NAME} previous and new address is the same.')
            return
    else:
        await store.sql_update_user(str(ctx.message.author.id), wallet_address, COIN_NAME)
        await ctx.message.add_reaction(EMOJI_OK_HAND)
        await ctx.send(f'{ctx.author.mention} You have registered {COIN_NAME} withdraw address.\n'
                       f'You can use `{server_prefix}withdraw AMOUNT {COIN_NAME}` anytime.')
        return


@bot.command(pass_context=True, help=bot_help_withdraw)
async def withdraw(ctx, amount: str, coin: str):
    global WITHDRAW_IN_PROCESS, IS_RESTARTING
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    botLogChan = bot.get_channel(id=config.discord.logChan)
    amount = amount.replace(",", "")

    try:
        amount = float(amount)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid given amount for command withdraw.')
        return

    COIN_NAME = coin.upper()

    if COIN_NAME not in ENABLE_COIN:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Unknown Ticker.')
        return

    if not is_withdrawable_coin(COIN_NAME):
        msg = await ctx.send(f'{EMOJI_ERROR} {ctx.author.mention} **WITHDRAW** is currently disable for {COIN_NAME}.')
        await msg.add_reaction(EMOJI_OK_BOX)
        return

    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    if is_maintenance_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {COIN_NAME} in maintenance.')
        return

    # add redis action
    random_string = str(uuid.uuid4())
    await add_tx_action_redis(json.dumps([random_string, "WITHDRAW", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "START"]), False)

    server_prefix = await get_guild_prefix(ctx)
    COIN_DEC = get_decimal(COIN_NAME)
    MinTx = get_min_tx_amount(COIN_NAME)
    MaxTX = get_max_tx_amount(COIN_NAME)
    real_amount = int(amount * COIN_DEC) if (coin_family == "TRTL" or coin_family == "XMR") else amount
    NetFee = get_reserved_fee(COIN_NAME)

    user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)
    if user_from is None:
        user_from = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME)

    CoinAddress = None
    if user_from['user_wallet_address'] is None:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not have a withdrawal address, please use '
                       f'`{server_prefix}register wallet_address` to register.')
        return
    else:
        CoinAddress = user_from['user_wallet_address']

    if coin_family == "TRTL":
        # Check available balance
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])

        if real_amount + NetFee > user_from['actual_balance']:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient reserved fee to withdraw '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME} to {CoinAddress}.')

            return

        if real_amount > MaxTX:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transactions cannot be bigger than '
                           f'{num_format_coin(MaxTX, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        elif real_amount < MinTx:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transactions cannot be smaller than '
                           f'{num_format_coin(MinTx, COIN_NAME)} '
                           f'{COIN_NAME}.')

            return

        sending = None
        if ctx.message.author.id not in WITHDRAW_IN_PROCESS:
            WITHDRAW_IN_PROCESS.append(ctx.message.author.id)
            try:
                sending = await store.sql_external_cn_xmr_single('DISCORD', str(ctx.message.author.id), real_amount, CoinAddress, COIN_NAME)
                tip_tx_tipper = "Transaction hash: `{}`".format(sending['transactionHash'])
                tip_tx_tipper += "\nTx Fee: `{}{}`".format(num_format_coin(sending['fee'], COIN_NAME), COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "WITHDRAW", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            WITHDRAW_IN_PROCESS.remove(ctx.message.author.id)
        if sending:
            await ctx.message.add_reaction(EMOJI_OK_BOX)
            await ctx.message.author.send(f'{EMOJI_ARROW_RIGHTHOOK} You have withdrawn {num_format_coin(real_amount, COIN_NAME)} '
                                          f'{COIN_NAME} '
                                          f'to `{CoinAddress}`\n'
                                          f'{tip_tx_tipper}')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{ctx.author.mention} Can not deliver TX for {COIN_NAME} right now. Try again soon.')
            return
    elif coin_family == "XMR":
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')

        # If balance 0, no need to check anything
        if float(user_from['actual_balance']) + float(userdata_balance['Adjust']) <= 0:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Please check your **{COIN_NAME}** balance.')
            return
        if real_amount > float(user_from['actual_balance']) + float(userdata_balance['Adjust']):
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to send out '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return

        NetFee = await get_tx_fee_xmr(coin = COIN_NAME, amount = real_amount, to_address = CoinAddress)
        if NetFee is None:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not get fee from network for: '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}. Please try again later in a few minutes.')
            return
        if real_amount + NetFee > float(user_from['actual_balance']) + float(userdata_balance['Adjust']):
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to send out '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}. You need to leave at least network fee: {num_format_coin(NetFee, COIN_NAME)}{COIN_NAME}')
            return
        if real_amount < MinTx:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be smaller than '
                           f'{num_format_coin(MinTx, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        if real_amount > MaxTX:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be bigger than '
                           f'{num_format_coin(MaxTX, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return

        SendTx = None
        if ctx.message.author.id not in WITHDRAW_IN_PROCESS:
            WITHDRAW_IN_PROCESS.append(ctx.message.author.id)
            try:
                SendTx = await store.sql_external_cn_xmr_single('DISCORD', str(ctx.message.author.id), real_amount,
                                                                CoinAddress, COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "WITHDRAW", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            WITHDRAW_IN_PROCESS.remove(ctx.message.author.id)
        else:
            # reject and tell to wait
            msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You have another tx in process. Please wait it to finish. ')
            
            return
        if SendTx:
            SendTx_hash = SendTx['tx_hash']
            await ctx.message.add_reaction(EMOJI_OK_BOX)
            await ctx.message.author.send(f'{EMOJI_ARROW_RIGHTHOOK} You have withdrawn {num_format_coin(real_amount, COIN_NAME)} '
                                          f'{COIN_NAME} to `{CoinAddress}`.\n'
                                          f'Transaction hash: `{SendTx_hash}`\n'
                                          'Network fee deducted from your account balance.')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            return
        return
    elif coin_family == "DOGE":
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')        

        real_amount = float(amount)
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        if real_amount + NetFee > float(user_from['actual_balance']) + float(userdata_balance['Adjust']):
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to send out '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        if real_amount < MinTx:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be smaller than '
                           f'{num_format_coin(MinTx, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        if real_amount > MaxTX:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be bigger than '
                           f'{num_format_coin(MaxTX, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        SendTx = None
        if ctx.message.author.id not in WITHDRAW_IN_PROCESS:
            WITHDRAW_IN_PROCESS.append(ctx.message.author.id)
            try:
                SendTx = await store.sql_external_doge('DISCORD', str(ctx.message.author.id), real_amount, NetFee,
                                                       CoinAddress, COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "WITHDRAW", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            WITHDRAW_IN_PROCESS.remove(ctx.message.author.id)
        else:
            msg = await ctx.send(f'{EMOJI_ERROR} {ctx.author.mention} You have another tx in progress.')
            await msg.add_reaction(EMOJI_OK_BOX)
            return
        if SendTx:
            await ctx.message.author.send(f'{EMOJI_ARROW_RIGHTHOOK} You have withdrawn {num_format_coin(real_amount, COIN_NAME)} '
                                          f'{COIN_NAME} to `{CoinAddress}`.\n'
                                          f'Transaction hash: `{SendTx}`\n'
                                          'Network fee deducted from the amount.')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            return


# Let's run balance update by a separate process
async def update_balance():
    INTERVAL_EACH = 10
    while True:
        print('sleep in second: '+str(INTERVAL_EACH))
        for coinItem in ["BTC", "DOGE", "LTC", "WRKZ", "DEGO", "TRTL", "XMR"]:
            await asyncio.sleep(INTERVAL_EACH)
            print('Update balance: '+ coinItem)
            start = time.time()
            try:
                await store.sql_update_balances(coinItem)
            except Exception as e:
                print(e)
            end = time.time()


# Notify user
async def notify_new_tx_user():
    INTERVAL_EACH = 10
    while True:
        pending_tx = await store.sql_get_new_tx_table('NO', 'NO')
        if pending_tx and len(pending_tx) > 0:
            # let's notify_new_tx_user
            for eachTx in pending_tx:
                user_tx = None
                if eachTx['coin_name'] not in ["DOGE"]:
                    user_tx = await store.sql_get_userwallet_by_paymentid(eachTx['payment_id'], eachTx['coin_name'])
                else:
                    user_tx = await store.sql_get_userwallet_by_paymentid(eachTx['payment_id'], eachTx['coin_name'])
                if user_tx:
                    user_found = bot.get_user(id=int(user_tx['user_id']))
                    if user_found:
                        is_notify_failed = False
                        try:
                            msg = None
                            if eachTx['coin_name'] not in ["DOGE", "LTC", "BTC", "DASH", "BCH"]:
                                msg = "You got a new deposit: ```" + "Coin: {}\nTx: {}\nAmount: {}\nHeight: {:,.0f}".format(eachTx['coin_name'], eachTx['txid'], num_format_coin(eachTx['amount'], eachTx['coin_name']), eachTx['height']) + "```"
                            else:
                                msg = "You got a new deposit: ```" + "Coin: {}\nTx: {}\nAmount: {}\nBlock Hash: {}".format(eachTx['coin_name'], eachTx['txid'], num_format_coin(eachTx['amount'], eachTx['coin_name']), eachTx['blockhash']) + "```"
                            await user_found.send(msg)
                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                            is_notify_failed = True
                            pass
                        update_notify_tx = await store.sql_update_notify_tx_table(eachTx['payment_id'], user_tx['user_id'], user_found.name, 'YES', 'NO' if is_notify_failed == False else 'YES')
                    else:
                        print('Can not find user id {} to notification tx: {}'.format(user_tx['user_id'], eachTx['txid']))
        else:
            print('No tx for notification')
        print('Sleep {}s'.format(INTERVAL_EACH))
        await asyncio.sleep(INTERVAL_EACH)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.NoPrivateMessage):
        await ctx.send('This command cannot be used in private messages.')
    elif isinstance(error, commands.DisabledCommand):
        await ctx.send('Sorry. This command is disabled and cannot be used.')
    elif isinstance(error, commands.MissingRequiredArgument):
        #command = ctx.message.content.split()[0].strip('.')
        #await ctx.send('Missing an argument: try `.help` or `.help ' + command + '`')
        pass
    elif isinstance(error, commands.CommandNotFound):
        pass


async def is_owner(ctx):
    return ctx.author.id == config.discord.ownerID


def is_maintenance():
    global redis_conn
    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:MAINTENANCE'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def set_maintenance(set_maint: bool = True):
    global redis_conn
    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:MAINTENANCE'
        if set_maint == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


# function to return if input string is ascii
def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def is_depositable_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:COIN_' + COIN_NAME + '_DEPOSIT'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def set_depositable_coin(coin: str, set_deposit: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:COIN_' + COIN_NAME + '_DEPOSIT'
        if set_deposit == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_withdrawable_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:COIN_' + COIN_NAME + '_WITHDRAW'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def set_withdrawable_coin(coin: str, set_withdraw: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:COIN_' + COIN_NAME + '_WITHDRAW'
        if set_withdraw == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_maintenance_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:COIN_' + COIN_NAME + '_MAINT'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def set_maintenance_coin(coin: str, set_maint: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'CryptoBountyBot:COIN_' + COIN_NAME + '_MAINT'
        if set_maint == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def store_action_list():
    while True:
        interval_action_list = 60
        try:
            openRedis()
            key = "CryptoBountyBot:ACTIONTX"
            if redis_conn and redis_conn.llen(key) > 0 :
                temp_action_list = []
                for each in redis_conn.lrange(key, 0, -1):
                    temp_action_list.append(tuple(json.loads(each)))
                num_add = store.sql_add_logs_tx(temp_action_list)
                if num_add > 0:
                    redis_conn.delete(key)
                else:
                    print(f"Failed delete {key}")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        await asyncio.sleep(interval_action_list)


async def add_tx_action_redis(action: str, delete_temp: bool = False):
    try:
        openRedis()
        key = "CryptoBountyBot:ACTIONTX"
        if redis_conn:
            if delete_temp:
                redis_conn.delete(key)
            else:
                redis_conn.lpush(key, action)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def get_guild_prefix(ctx):
    if isinstance(ctx.channel, discord.DMChannel) == True: return "."
    serverinfo = store.sql_info_by_server(str(ctx.guild.id))
    if serverinfo is None:
        return "."
    else:
        return serverinfo['prefix']


def get_cn_coin_from_address(CoinAddress: str):
    COIN_NAME = None
    if CoinAddress.startswith("Wrkz"):
        COIN_NAME = "WRKZ"
    elif CoinAddress.startswith("dg"):
        COIN_NAME = "DEGO"
    elif CoinAddress.startswith("TRTL"):
        COIN_NAME = "TRTL"
    elif (CoinAddress.startswith("4") or CoinAddress.startswith("8") or CoinAddress.startswith("5") or CoinAddress.startswith("9")) \
        and (len(CoinAddress) == 95 or len(CoinAddress) == 106):
        addr = None
        # Try XMR
        try:
            addr = address_xmr(CoinAddress)
            COIN_NAME = "XMR"
            return COIN_NAME
        except Exception as e:
            # traceback.print_exc(file=sys.stdout)
            pass
    elif CoinAddress.startswith("D") and len(CoinAddress) == 34:
        COIN_NAME = "DOGE"
    elif (CoinAddress[0] in ["M", "L"]) and len(CoinAddress) == 34:
        COIN_NAME = "LTC"
    elif (CoinAddress[0] in ["3", "1"]) and len(CoinAddress) == 34:
        COIN_NAME = "BTC"
    print('CryptoBountyBot get_cn_coin_from_address return {}: {}'.format(CoinAddress, COIN_NAME))
    return COIN_NAME


@register.error
async def register_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing your wallet address. '
                       f'You need to have a supported coin **address** after `register` command. Example: {prefix}register coin_address')
    return


@withdraw.error
async def withdraw_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing amount and/or ticker. '
                       f'You need to tell me **AMOUNT** and/or **TICKER**.\nExample: {prefix}withdraw **1,000 coin_name**')
    return


@bounty.error
async def bounty_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing argument. '
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty subcommand')
    return


@bounty.error
@detail.error
async def bounty_detail_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing argument. '
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty detail')
    return


@bounty.error
@mylist.error
async def bounty_mylist_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing argument. '
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty mylist')
    return


@bounty.error
@end.error
async def bounty_detail_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing argument. '
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty end')
    return


@bounty.error
@apply.error
async def bounty_detail_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing argument. '
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty apply')
    return


@bounty.error
@cancel_apply.error
async def bounty_cancel_apply_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing argument. '
                       f'Please use help of each subcommand. Example: {prefix}help bounty OR {prefix}help bounty cancel_apply')
    return


def randomString(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


@click.command()
def main():
    bot.loop.create_task(update_balance())
    bot.loop.create_task(notify_new_tx_user())
    bot.loop.create_task(store_action_list())
    bot.run(config.discord.token, reconnect=True)


if __name__ == '__main__':
    main()