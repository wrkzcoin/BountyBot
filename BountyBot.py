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

# MySQL
import pymysql, pymysqlpool
import pymysql.cursors


from typing import List, Dict

redis_pool = None
redis_conn = None
redis_expired = 600

EMOJI_HOURGLASS_NOT_DONE = "\u23F3"
EMOJI_ERROR = "\u274C"
EMOJI_FLOPPY = "\U0001F4BE"
EMOJI_OK_BOX = "\U0001F197"
EMOJI_MAINTENANCE = "\U0001F527"

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

bot_help_admin_shutdown = "Restart bot."
bot_help_admin_maintenance = "Bot to be in maintenance mode ON / OFF"

bot = AutoShardedBot(command_prefix=['.', 'btb.', 'btb!'], owner_id = config.discord.ownerID, case_insensitive=True)
bot.remove_command('help')


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
@admin.command(aliases=['maintenance'], help=bot_help_admin_maintenance)
async def maint(ctx):
    botLogChan = bot.get_channel(id=config.discord.logChan)
    if is_maintenance():
        await ctx.send(f'{EMOJI_OK_BOX} bot maintenance **OFF**.')
        set_maint = set_maintenance(False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} bot maintenance **ON**.')
        set_maint = set_maintenance(True)
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


@click.command()
def main():
    bot.run(config.discord.token, reconnect=True)


if __name__ == '__main__':
    main()