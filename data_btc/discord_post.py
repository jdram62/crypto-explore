#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import requests

ABSOLUTE_PATH = os.path.dirname(os.path.abspath(__file__))
DISCORD_WEBHOOKS = []
with open(os.path.join(ABSOLUTE_PATH, f'disc_hook.txt'), 'r') as f:
    for line in f:
        DISCORD_WEBHOOKS.append(line[:-1])

VD_MARKETS = set(('binance_perp_btcusdt', 'binance_spot_btcusdt', 'coinbase_spot_btcusd', 'bybit_perp_btcusdt'))


def post_vd(vol_deltas):
    vd_spot = 0
    vd_perp = 0
    for market, vd in vol_deltas.items():
        if 'spot' in market:
            vd_spot += vd
        else:
            vd_perp += vd

    msg = f'spot:{round(vd_spot, 2)},perp:{round(vd_perp, 2)}'

    payload = {'username': 'botty',
               'content': msg}

    r = requests.post(url=DISCORD_WEBHOOKS[0], json=payload)



def post_fr(funding):
    payload = {'username': 'botty',
               'content': f'{funding:.5f}'}

    r = requests.post(url=DISCORD_WEBHOOKS[1], json=payload)


def post_ohlc(ohlc):
    msg = ohlc.replace('[', '').replace(']', '').replace("'", '')
    payload = {'username': 'botty',
               'content': msg}

    r = requests.post(url=DISCORD_WEBHOOKS[2], json=payload)
