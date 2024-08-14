import asyncio
import pandas as pd
import datetime as dt
import re

from fr24.core import FR24
from questdb.ingress import Sender

def int2hexcode(int): 
     if int == 0:
          return '------'
     else:
          return hex(int).replace('0x','').upper().rjust(6,'0')
def callsign(cs):
     if cs.strip() =='':
          return '------'
     else:
          return cs
def int2frid(int): 
     return hex(int).replace('0x','')
def source(int):
     return
def ymd(ts): 
     return dt.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')

def al(cs):
     x=re.findall("^((RA|UK)[0-9]{5}|V8001|T7[0-9]|B[0-9]{4,5}|^N([0-9]{1,5})([A-Z]{0,2})?)",cs)
     if len(x) > 0:
          return ''
     x=re.findall("^([A-Z]{2,10})[0-9]",cs)
     if len(x) == 1:
          return x[0]
     else:
          return ''


async def my_feed() -> None:
    conf = 'http::addr=host3.nzaa.de:49000;username=flt;password=qFlightRadar_16_;'

    async with FR24() as fr24:
        await fr24.login()
        response = await fr24.live_feed.fetch(fields=["flight","reg","type","icao_address","route"])
        # print(response.data)
        datac = response.to_arrow()
        df=datac.df
        print(type(df));
        df['hex']=df['icao_address'].apply(int2hexcode)
        df['frid']=df['flightid'].apply(int2frid)
        df['ymd']=df['timestamp'].apply(ymd)
        df['cs']=df['cs'].apply(callsign)
        df['ts']=df['timestamp']
        df['al']=df['cs'].apply(al)
        df['timestamp']=pd.to_datetime(df['timestamp'], unit='s')
        cols = list(df.columns.values) #Make a list of all of the columns in the df
        df.drop('icao_address',axis=1, inplace=True)
        df.drop('flightid',axis=1, inplace=True)
        print(df)
        #datac.save()
#        print(datac.df.to_parquet())
        with Sender.from_conf(conf) as sender:
            sender.dataframe(df, table_name="fr24", at="timestamp")
            sender.flush()
            
# await my_feed()

asyncio.run(my_feed())
