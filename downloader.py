import os
import sys
import argparse
from websocket import create_connection
import json
import time
import queue
import threading
import ast
from tabulate import tabulate
import pandas as pd
from collections import defaultdict

SOCKET_URI = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

SUPPORTED_PRODUCTS = ['BTC-USD']

subscribe_message = {
        "type": "subscribe",
        "product_ids": SUPPORTED_PRODUCTS,
        "channels": ["level2_batch"]
    }

# Queue to hold messages
message_queue = queue.Queue()

D = {'tz':[], 'highest_bid': [], 'lowest_ask': [],'difference': [], 'highest_bid_qty': [], 'lowest_ask_qty': [], 'mid_price': []}

def transform_message(message):
    result = json.loads(message)
    if 'type' in result and result['type'] == 'l2update' and 'product_id' in result and result['changes'][0][2] != '0.00000000' and result['changes'][0][1] != None:
        # Extract data from the websocket message
        tz = result['time']
        product_id = result['product_id']
        side = ['ask' if result['changes'][0][0] == 'sell' else 'bid'][0]
        price_level = float(result['changes'][0][1])
        quantity = float(result['changes'][0][2])

        # Generate new message in required format
        transformed_message = json.dumps({
            "tz": tz,
            "product_id": product_id,
            "side": side,
            "price_level": price_level,
            "quantity": quantity,
        })

        return transformed_message.encode("utf-8").decode("utf-8")

def get_min_ask_price(data):
    # Filter out dictionaries where 'side' is not 'ask'
    ask_orders = [d for d in data if d['side'] == 'ask']

    if not ask_orders:
        return 'no asks'

    # Find the dictionary with the lowest 'price_level'
    min_ask_order = min(ask_orders, key=lambda x: x['price_level'])

    return min_ask_order['price_level']

def get_max_bid_price(data):
    # Filter out dictionaries where 'side' is not 'ask'
    bid_orders = [d for d in data if d['side'] == 'bid']

    if not bid_orders:
        return "no bids"

    # Find the dictionary with the lowest 'price_level'
    max_bid_order = max(bid_orders, key=lambda x: x['price_level'])

    return max_bid_order['price_level']

def find_qty_with_max_price(data):
    # Get all quantities where the price level is the maximum
    quantities = [d['quantity'] for d in data if d['price_level'] == get_max_bid_price(data)]
    return quantities

def find_qty_with_min_price(data):
    # Get all quantities where the price level is the maximum
    quantities = [d['quantity'] for d in data if d['price_level'] == get_min_ask_price(data)]
    return quantities

def process_batch_messages(interval):
    while True:
        batch = [] # your batch
        start_time = time.time()
        while time.time() - start_time < interval:
            if not message_queue.empty():
                batch.append(ast.literal_eval(message_queue.get()))

            batch_five = defaultdict(list)
            for d in batch:
                for key, value in d.items():
                    batch_five[key].append(value)

            dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
            max_bid = get_max_bid_price(batch)
            min_ask = get_min_ask_price(batch)

            batch_five["tz"] = dt
            batch_five["highest_bid"] = max_bid
            batch_five["lowest_ask"] = min_ask

            if isinstance(max_bid, str) or isinstance(min_ask, str):
                batch_five["difference"] = 'n/a'
            else:
                batch_five["difference"] = abs(max_bid-min_ask)

            batch_five['highest_bid_qty'] = str(find_qty_with_max_price(batch))
            batch_five["lowest_ask_qty"] = str(find_qty_with_min_price(batch))

            if isinstance(get_max_bid_price(batch), str) or isinstance(get_min_ask_price(batch), str):
                batch_five["mid_price"] = 'n/a'
            else:
                batch_five["mid_price"] = (max_bid+min_ask) / 2

        # Fill dictionary D where we accumulate results
        for key in D.keys():
            D[key].append(batch_five[key])
        # Create new tempo df to calculate another metrics for 1m, 5m, 15m from
        accumulate_df = pd.DataFrame(D)
        accumulate_df['tz'] = pd.to_datetime(accumulate_df['tz'])
        accumulate_df.set_index('tz', inplace=True)
        #replace current difference with a max observed difference
        accumulate_df['difference'] = accumulate_df['difference'].max()

        accumulate_df['mid_price'] = pd.to_numeric(accumulate_df['mid_price'], errors='coerce')
        accumulate_df['avg_mid_price_1m'] = accumulate_df["mid_price"].resample('1T').mean().mean()
        accumulate_df['avg_mid_price_5m'] = accumulate_df["mid_price"].resample('5T').mean().mean()
        accumulate_df['avg_mid_price_15m'] = accumulate_df["mid_price"].resample('15T').mean().mean()
        # print(f'\n{insights_df}\n\n')

        print(f'Insights at {dt} are: \n {tabulate(accumulate_df.tail(1), headers="keys", tablefmt="psql")} \n')

def validate_product_id(product_id):
    if product_id.upper() not in SUPPORTED_PRODUCTS:
        raise argparse.ArgumentTypeError(f"Unsupported product '{product_id.upper()}'. Supported products are {SUPPORTED_PRODUCTS}")
    return product_id.upper()

def main():
    '''Ask user for a crypto product_id they are interested in'''
    parser = argparse.ArgumentParser(description="Generate insights for a provided crypto product.")
    parser.add_argument("product_id", help="Provide crypto product to generate insights in format e.g. BTC-USD", type=validate_product_id, nargs="?")

    args = parser.parse_args()
    if args.product_id:
        product_id = args.product_id
    else:
        while True:
            try:
                product_id = input("Provide crypto product to generate insights in format e.g. BTC-USD :\n> ")
                product_id = validate_product_id(product_id)
                break
            except argparse.ArgumentTypeError as e:
                print(str(e))

    ws = None
    try:
        threading.Thread(target=process_batch_messages, args=(5,)).start()

        ws = create_connection(SOCKET_URI)
        print("WebSocket opened")
        ws.send(json.dumps(subscribe_message))

        while True:
            message = ws.recv()
            transformed_message = transform_message(message)
            if transformed_message is not None:
                message_queue.put(transformed_message)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        if ws:
            ws.close()
        print("\nWebSocket closed")


if __name__ == '__main__':
    try:
        main()
        while len(sys.argv) == 1:
            main()
    except KeyboardInterrupt:
        print('\nGoodbye!')
        sys.exit(0)
