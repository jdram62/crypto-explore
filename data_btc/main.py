#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import datetime
import functools
import hashlib
import hmac
import json
import os
import signal

import websockets
# from dotenv import load_dotenv

from discord_post import post_fr, post_ohlc, post_vd

# load_dotenv()
# CB_KEY=os.getenv('COINBASE_KEY')
# CB_SECRET=os.getenv('COINBASE_SECRET')
ABSOLUTE_PATH = os.path.dirname(os.path.abspath(__file__))
CB = []
with open(os.path.join(ABSOLUTE_PATH, f'cb.txt'), 'r') as f:
    for line in f:
        CB.append(line[:-1])
CB_KEY = CB[0]
CB_SECRET = CB[1]
VD_MARKETS = set(('binance_perp_btcusdt', 'binance_spot_btcusdt', 'coinbase_spot_btcusd', 'bybit_perp_btcusdt'))
FR_MARKETS = set(('binance_fr_btcusdt', 'bybit_fr_btcusdt'))


async def shutdown(_sig, _loop):
    print(f'Shutting down... {_sig.name}')
    tasks = [task for task in asyncio.all_tasks(loop=_loop) if task is not asyncio.current_task(loop=_loop)]
    list(map(lambda task: task.cancel(), tasks))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print(f'finished awaiting cancelled tasks, results: {results}')
    _loop.stop()


async def vol_delta_worker():
    next_candle_min = datetime.datetime.utcnow().minute + 1
    markets_recorded = set()
    record_vol_deltas = {symbol: 0 for symbol in VD_MARKETS}
    vol_deltas = {symbol: 0 for symbol in VD_MARKETS}
    try:
        while True:
            trade = await trades_queue.get()
            if trade['time'].minute >= next_candle_min or (trade['time'].minute <= 3 and next_candle_min == 60):
                if trade['symbol'] not in markets_recorded:
                    record_vol_deltas[trade["symbol"]] = vol_deltas[trade['symbol']]
                    vol_deltas[trade['symbol']] = 0
                    markets_recorded.add(trade['symbol'])
                if markets_recorded == VD_MARKETS:
                    next_candle_min = trade['time'].minute + 1
                    markets_recorded = set()
                    post_vd(record_vol_deltas)

            vol_deltas[trade['symbol']] = vol_deltas[trade['symbol']] + (trade['side'] * trade['price'] * trade['quantity'])

    except asyncio.CancelledError as e:
        print(f'vol_delta: {e}')

async def binance_ws_perp():
    uri = f'wss://fstream.binance.com/ws/btcusdt@aggTrade'
    try:
        async for websocket in websockets.connect(uri):
            try:
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    await trades_queue.put({'symbol': f'binance_perp_btcusdt',
                                            'time': datetime.datetime.fromtimestamp(data['T'] / 1000.0),
                                            'price': float(data['p']),
                                            'quantity': float(data['q']),
                                            'side': -1 if data['m'] else 1})  # side == true == sell == -1


            except websockets.ConnectionClosed as e:
                print(f'binance ws perp btcusdt: {e} : {datetime.datetime.utcnow()}')
                continue
    except asyncio.CancelledError as e:
        print(f'binance perp: {e}')


async def binance_ws_spot():
    uri = f'wss://stream.binance.com:9443/ws/btcusdt@aggTrade'
    try:
        async for websocket in websockets.connect(uri):
            try:
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    await trades_queue.put({'symbol': f'binance_spot_btcusdt',
                                            'time': datetime.datetime.fromtimestamp(data['T'] / 1000.0),
                                            'price': float(data['p']),
                                            'quantity': float(data['q']),
                                            'side': -1 if data['m'] else 1})


            except websockets.ConnectionClosed as e:
                print(f'binance ws perp btcusdt: {e} : {datetime.datetime.utcnow()}')
                continue
    except asyncio.CancelledError as e:
        print(f'binance perp: {e}')


async def bybit_ws_perp():
    uri = 'wss://stream.bybit.com/v5/public/linear'
    sub = json.dumps({'op': 'subscribe',
                      'args': ['publicTrade.BTCUSDT']})

    try:
        async for websocket in websockets.connect(uri):
            try:
                await websocket.send(sub)
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    if 'data' in data:
                        for trade in data['data']:
                            await trades_queue.put({'symbol': 'bybit_perp_btcusdt',
                                                    'time': datetime.datetime.fromtimestamp(int(trade['T']) / 1000),
                                                    'price': float(trade['p']),
                                                    'quantity': float(trade['v']),
                                                    'side': 1 if trade['S'] == 'Buy' else -1})
                                
            except websockets.ConnectionClosed as e:
                print(f'bybit perp: {e} : {datetime.datetime.utcnow()}')
                continue

    except asyncio.CancelledError as e:
        print('bybit perp: ', e)


async def coinbase_ws_spot():
    uri = 'wss://advanced-trade-ws.coinbase.com'   

    try:
        async for websocket in websockets.connect(uri):
            try:
                sub = json.dumps(cb_sub())
                await websocket.send(sub)
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    if data['channel'] == 'market_trades':
                        for trade in data['events'][0]['trades']:
                            await trades_queue.put({'symbol': 'coinbase_spot_btcusd',
                                                    'time': datetime.datetime.fromisoformat(trade['time'].split('.')[0].replace('Z', '')),
                                                    'price': float(trade['price']),
                                                    'quantity': float(trade['size']),
                                                    'side': 1 if trade['side'] == 'SELL' else -1})
            except websockets.ConnectionClosed as e:
                print(f'cb spot: {e} : {datetime.datetime.utcnow()}')
                continue

    except asyncio.CancelledError as e:
        print('cb spot: ', e)

async def avg_fr_worker():
    next_candle_min = datetime.datetime.utcnow().minute + 1
    markets_recorded = set()
    funding_rates = {} 

    try:
        while True:
            fr_q = await fundings_queue.get()
            if fr_q['time'].minute >= next_candle_min or (fr_q['time'].minute <= 3 and next_candle_min == 60):
                if fr_q['symbol'] not in markets_recorded:
                    funding_rates[fr_q['symbol']] = fr_q['funding']
                    markets_recorded.add(fr_q['symbol'])

                if markets_recorded == FR_MARKETS:
                    next_candle_min = fr_q['time'].minute + 1
                    markets_recorded = set()
                    avg_funding = sum(funding_rates.values()) / len(funding_rates.values())
                    post_fr(avg_funding*100)

    except asyncio.CancelledError as e:
        print(f'vol_delta: {e}')


async def binance_ws_fr():
    uri = f'wss://fstream.binance.com/ws/btcusdt@markPrice@1s'
    try:
        async for websocket in websockets.connect(uri):
            try:
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    
                    await fundings_queue.put({'symbol': 'binance_fr_btcusdt',
                                              'time': datetime.datetime.fromtimestamp(data['E'] / 1000.0),
                                              'funding': float(data['r'])})

            except websockets.ConnectionClosed as e:
                print(f'binance ws perp btcusdt: {e} : {datetime.datetime.utcnow()}')
                continue
    except asyncio.CancelledError as e:
        print(f'funding ws: {e}')


async def bybit_ws_fr():
    uri = 'wss://stream.bybit.com/v5/public/linear'
    sub = json.dumps({'op': 'subscribe',
                      'args': ['tickers.BTCUSDT']})

    try:
        async for websocket in websockets.connect(uri):
            try:
                fr = None
                await websocket.send(sub)
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    if 'data' in data:
                        if data['data'].get('fundingRate'):
                            fr = float(data['data']['fundingRate'])

                        await fundings_queue.put({'symbol': 'bybit_fr_btcusdt',
                                                  'time': datetime.datetime.fromtimestamp(int(data['ts']) / 1000),
                                                  'funding': fr})
                                                        
            except websockets.ConnectionClosed as e:
                print(f'bybit perp: {e} : {datetime.datetime.utcnow()}')
                continue

    except asyncio.CancelledError as e:
        print('bybit perp: ', e)



async def ohlc_worker():
    try:
        while True:
            ohlc = await ohlc_queue.get()
            post_ohlc(str(list(ohlc.values())))

    except asyncio.CancelledError as e:
        print(f'vol_delta: {e}')



async def binance_ws_ohlc():
    uri = f'wss://fstream.binance.com/ws/btcusdt@kline_1m'
    try:
        async for websocket in websockets.connect(uri):
            try:
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    if data['k']['x'] == True:
                        await ohlc_queue.put({'o': data['k']['o'],
                                              'h': data['k']['h'],
                                              'l': data['k']['l'],
                                              'c': data['k']['c']})

            except websockets.ConnectionClosed as e:
                print(f'binance ws perp btcusdt: {e} : {datetime.datetime.utcnow()}')
                continue
    except asyncio.CancelledError as e:
        print(f'funding ws: {e}')



def cb_sub():
    timestamp = str(int(datetime.datetime.now().timestamp()))
    message = timestamp + 'market_trades' + 'BTC-USD'
    signature = hmac.new(CB_SECRET.encode('utf-8'), message.encode('utf-8'), digestmod=hashlib.sha256).digest().hex()
    return {'type': 'subscribe',
            'product_ids': ['BTC-USD'],
            'channel': 'market_trades',
            'api_key': CB_KEY,
            'timestamp': timestamp,
            'signature': signature}


if __name__ == '__main__':
    trades_queue = asyncio.Queue()
    fundings_queue = asyncio.Queue()
    ohlc_queue = asyncio.Queue()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, functools.partial(asyncio.ensure_future, shutdown(signal.SIGINT, loop)))

    loop.create_task(vol_delta_worker())
    loop.create_task(binance_ws_perp())
    loop.create_task(binance_ws_spot())
    loop.create_task(coinbase_ws_spot())
    loop.create_task(bybit_ws_perp())
    
    loop.create_task(avg_fr_worker())
    loop.create_task(binance_ws_fr())
    loop.create_task(bybit_ws_fr())

    loop.create_task(ohlc_worker())
    loop.create_task(binance_ws_ohlc())
    try:
        loop.run_forever()
    finally:
        print(loop.close())
        print('by')
