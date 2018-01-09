#!/usr/bin/env python

import os
import requests
import json
import time
import pytz
from tzlocal import get_localzone
import datetime
import random
from math import floor, ceil
import logging
from logging.handlers import RotatingFileHandler


apiurl = "https://api.coinmarketcap.com/v1/ticker/"
pullIntervalSecs = 5*60

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('conmarketcapapi')
handler = logging.handlers.RotatingFileHandler(os.path.join('.','..','..','logs','coinmarketcap.log'), maxBytes=150000, backupCount=100)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

logger.info('Script starting')

# get local timezone    
localTZ = get_localzone()

while True:

    # Pull from API
    timePulledStr = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    apiResponse = requests.get(apiurl)

    # Parse response
    coinData = []
    if(apiResponse.ok):
        logger.info('pulling')
        respData = json.loads(apiResponse.content)
    
        if isinstance(respData,list) and len(respData)>0:
        
            # Add each coin
            for respCoin in respData:
                try:
                    # Append new dict
                    coinData.append({'id':respCoin['symbol']})
                
                    # Add data
                    coinData[-1]['price_usd'] = respCoin['price_usd']
                    coinData[-1]['volume_24h_usd'] = respCoin['24h_volume_usd']
                    coinData[-1]['supply_avail'] = respCoin['available_supply']
                    coinData[-1]['supply_total'] = respCoin['total_supply']
                    coinData[-1]['supply_max'] = respCoin['max_supply']
                
                    # Updated time is Unix timestamp in local time, convert to UTC
                    updatedDTlocal = localTZ.localize(datetime.datetime.fromtimestamp(int(respCoin['last_updated'])))
                    updatedDTutc = updatedDTlocal.astimezone(pytz.utc)
                    coinData[-1]['time_updated'] = updatedDTutc.strftime('%Y%m%dT%H%M%SZ')
                except Exception:
                    logger.error('Exception pulling coin data', exc_info=True)
                
        else:
            # Not a list
            logger.warning('API response not a list')
    else:
        # API response not 200
        apiResponse.raise_for_status()
        logger.warning('API response not OK [{0}]'.format(r.status_code))

    # Write data as json
    if coinData:
        filePath = os.path.join('.','..','..','data','coinmarketcap','coinmarketcap_'+timePulledStr+'.json')
        with open(filePath, 'w') as jsonFile:
            json.dump(coinData, jsonFile)

    # Wait for next pull
    time.sleep(pullIntervalSecs + random.randint(floor(-pullIntervalSecs/3),ceil(pullIntervalSecs/3)))
    handler.flush()















