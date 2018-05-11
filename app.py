import asyncio
import os
import argparse
import zmq
import zmq.asyncio
import json
import logging
import time
import traceback
import re
import sys
import csv
import struct
import aiohttp
import aiopusher
from numbers import Number
from zmapi import fix
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime
from datetime import datetime
from zmapi.zmq.utils import *
from zmapi.utils import random_str, delayed, get_timestamp
from zmapi.controller import RESTConnectorCTL, ConnectorCTL
from zmapi.logging import setup_root_logger, disable_logger
from zmapi.exceptions import *
from collections import defaultdict
from uuid import uuid4


################################## CONSTANTS ##################################


CAPABILITIES = sorted([
    # "UNSYNC_SNAPSHOT",
    "GET_TICKER_FIELDS",
    "SUBSCRIBE",
    "LIST_DIRECTORY",
    "PUB_ORDER_BOOK_INCREMENTAL",
])


TICKER_FIELDS = [
#    {"field": "symbol",
#     "type": "str",
#     "label": "Symbol",
#     "description": "The symbol of the ticker"}
]

MODULE_NAME = "ddex.io-md"
ENDPOINT_NAME = "ddex.io"


################################ GLOBAL STATE #################################


class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"
g.session_id = str(uuid4())
g.seq_no = 0

g.startup_event = asyncio.Event()

# placeholder for Logger
L = logging.root


###############################################################################


class MyController(RESTConnectorCTL):


    def __init__(self, sock_dn, ctx):
        super().__init__(sock_dn, ctx)
        self._add_throttler(r".*", 6000, 10 * 60)
        self._base_url = "https://api.ddex.io/v2/"


    def _process_fetched_data(self, data, url):
        data = json.loads(data.decode())
        if data["status"] != 0:
            err_str = "error fetching {}: status={}, desc='{}'"
            err_str = err_str.format(url, data["status"], data["desc"])
            raise BusinessMessageRejectException(err_str)
        return data["data"]


    async def ZMGetStatus(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetStatusResponse
        d = {}
        d["module_name"] = MODULE_NAME
        d["endpoint_name"] = ENDPOINT_NAME
        d["session_id"] = g.session_id
        res["Body"] = [d]
        return res


    async def ZMListDirectory(self, ident, msg_raw, msg):
        url = self._base_url + "markets"
        data = await self._http_get_cached(url, 3600)
        data = data["markets"]
        data = sorted(data, key=lambda x: x["id"])
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListDirectoryResponse
        res["Body"] = body = {}
        group = []
        for x in data:
            d = {}
            d["ZMNodeName"] = x["id"]
            d["ZMInstrumentID"] = x["id"]
            group.append(d)
        body["ZMDirEntries"] = group
        return res


    async def SecurityListRequest(self, ident, msg_raw, msg):

        body = msg["Body"]
        instrument_id = body.get("ZMInstrumentID")

        url = self._base_url + "markets"
        data = await self._http_get_cached(url, 3600)
        data = data["markets"]

        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.SecurityList
        res["Body"] = body = {}
        body["SecListGrp"] = group = []

        if instrument_id:
            data = [x for x in data if x["id"] == instrument_id]
            if not data:
                body["SecurityRequestResult"] = \
                        fix.SecurityRequestResult.NoInstrumentsFound
                return res
        
        for x in data:
            d = {}
            d["SecurityDesc"] = d["ZMInstrumentID"] = x["id"]
            d["MinPriceIncrement"] = 10 ** (-x["priceDecimals"])
            group.append(d)

        return res


    async def _get_snapshot(self, ins_id, sub_def):

        res = {}

        res["MDFullGrp"] = group = []
        ticks = sub_def["MDReqGrp"]
        market_depth = sub_def.get("MarketDepth", 0)
        if market_depth == 0:
            market_depth = sys.maxsize
        print(market_depth)
        agg_book = sub_def.get("AggregatedBook", True)

        get_ob = True
        if market_depth == 1 and "*" not in ticks \
                and "0" not in ticks and "1" not in ticks:
            get_ob = False

        # get_quotes = False
        # if "*" in ticks or \
        #         "7" in ticks or \
        #         "8" in ticks or \
        #         "2" in ticks or \
        #         "4" in ticks or \
        #         "B" in ticks:
        #     get_quotes = True

        async with aiohttp.ClientSession() as session:

            url = self._base_url + "markets/{}/ticker".format(ins_id)
            data = await self._http_get(url, session=session)
            data = data["ticker"]
            if "*" in ticks or "7" in ticks:
                d = {}
                d["MDEntryType"] = "7"
                d["MDEntryPx"] = float(data["high"])
                group.append(d)
            if "*" in ticks or "8" in ticks:
                d = {}
                d["MDEntryType"] = "8"
                d["MDEntryPx"] = float(data["low"])
            if "*" in ticks or "B" in ticks:
                d = {}
                d["MDEntryType"] = "B"
                d["MDEntrySize"] = float(data["volume"])
                group.append(d)
            if "*" in ticks or "2" in ticks:
                d = {}
                d["MDEntryType"] = "2"
                d["MDEntryPx"] = float(data["price"])
                group.append(d)
            ts = datetime.utcfromtimestamp(float(data["updatedAt"] / 1000))
            ts = int(ts.timestamp()) * 1000000000
            res["LastUpdateTime"] = ts

            if get_ob:
                url = self._base_url + "markets/{}/orderbook".format(ins_id)
                if agg_book:
                    if market_depth == 1:
                        level = 1  # aggregated bbo
                    else:
                        level = 2  # aggregated, all levels (?)
                else:
                    level = 3  # all levels, non-agg
                url += "?level={}".format(level)
                print(url)
                data = await self._http_get(url, session=session)
                data = data["orderBook"]
                bids = data.get("bids", [])
                bids = sorted(bids, key=lambda x: float(x["price"]))[::-1]
                asks = data.get("asks", [])
                asks = sorted(asks, key=lambda x: float(x["price"]))
                for i, lvl in enumerate(bids):
                    if agg_book and i >= market_depth:
                        break
                    d = {}
                    d["MDEntryType"] = "0"
                    d["MDEntryPx"] = float(lvl["price"])
                    d["MDEntrySize"] = float(lvl["amount"])
                    if agg_book:
                        d["MDPriceLevel"] = i + 1
                    else:
                        d["OrderID"] = lvl["orderId"]
                    group.append(d)
                for i, lvl in enumerate(asks):
                    if agg_book and i >= market_depth:
                        break
                    d = {}
                    d["MDEntryType"] = "1"
                    d["MDEntryPx"] = float(lvl["price"])
                    d["MDEntrySize"] = float(lvl["amount"])
                    if agg_book:
                        d["MDPriceLevel"] = i + 1
                    else:
                        d["OrderID"] = lvl["orderId"]
                    group.append(d)

        return res

    
    async def MarketDataRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        if srt not in "012":
            raise MarketDataRequestRejectException(
                    fix.UnsupportedSubscriptionRequestType, srt)
        ins_id = body["ZMInstrumentID"]
        ticks = body.get("MDReqGrp")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMMarketDataRequestResponse
        res["Body"] = body = {}
        if srt == "0":
            snap = await self._get_snapshot(ins_id, msg["Body"])
            body["ZMSnapshots"] = [snap]
        # elif srt == "1":
        #     if "*" in ticks or "2" in ticks:
        #         tres = await g.pub.subscribe_trades(ins_id)
        #     else:
        #         tres = await g.pub.unsubscribe_trades(ins_id)
        #     bres = await g.pub.subscribe_order_book(ins_id)
        #     body["Text"] = "Trades: {} | Book: {}".format(tres, bres)
        # elif srt == "2":
        #     tres = await g.pub.unsubscribe_trades(ins_id)
        #     bres = await g.pub.unsubscribe_order_book(ins_id)
        #     body["Text"] = "Trades: {} | Book: {}".format(tres, bres)
        return res


    async def ZMGetInstrumentFields(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetInstrumentFieldsResponse
        res["Body"] = TICKER_FIELDS
        return res

    
    async def ZMListCapabilities(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListCapabilitiesResponse
        res["Body"] = body = {}
        body["ZMCaps"] = CAPABILITIES
        return res


###############################################################################


class Publisher:


    def __init__(self, sock):
        self._sock = sock


    async def _restart(self):
        g.startup_event.set()


    async def run(self):
        await self._restart()


###############################################################################


def parse_args():
    parser = argparse.ArgumentParser(description="ddex.io md connector")
    parser.add_argument("ctl_addr", help="address to bind to for ctl socket")
    parser.add_argument("pub_addr", help="address to bind to for pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--log-websockets", action="store_true",
                        help="add websockets logger")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    if not args.log_websockets:
        disable_logger("websockets")
    setup_root_logger(args.log_level)


def init_zmq_sockets(args):
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr)


def main():
    args = parse_args()
    setup_logging(args)
    init_zmq_sockets(args)
    g.ctl = MyController(g.sock_ctl, g.ctx)
    g.pub = Publisher(g.sock_pub)
    L.debug("starting event loop ...")
    tasks = [
        delayed(g.ctl.run, g.startup_event),
        g.pub.run(),
    ]
    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass
    L.debug("destroying zmq context ...")
    g.ctx.destroy()


if __name__ == "__main__":
    main()

