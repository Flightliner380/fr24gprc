import asyncio
import pandas as pd
import datetime as dt
import re

from fr24.core import FR24
from questdb.ingress import Sender

def int2hexcode(int): 
     if int == 0:
          return '--------'
     else:
          return hex(int).replace('0x','').upper().rjust(6,'0')
     

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
     h = 'D0971'
     # print(h.rjust(6,'0') )
     print(al('BERN134FT'))
     print(al('V8001'))
     print(al('N35FT'))
     # print (int2hexcode (6190336))

asyncio.run(my_feed())