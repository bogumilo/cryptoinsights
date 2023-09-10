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

SOCKET_URI = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

SUPPORTED_PRODUCTS = ['BTC-USD']

subscribe_message = {
        "type": "subscribe",
        "product_ids": SUPPORTED_PRODUCTS,
        "channels": ["level2_batch"]
    }

# Queue to hold messages
message_queue = queue.Queue()

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

def find_extreme(input_df, side):
    df = input_df[input_df.eval("side == side")]
    if not df.empty:
        if side == 'bid':
            outcome = df.loc[df['price_level'].idxmax()]
        elif side == 'ask':
            outcome = df.loc[df['price_level'].idxmin()]
    else:
        print(f"No rows where 'side' is '{side}'")
        outcome = pd.DataFrame(columns=df.columns)
    return outcome

def process_batch_messages(interval):
    while True:
        batch = [] # your batch
        start_time = time.time()
        while time.time() - start_time < interval:
            if not message_queue.empty():
                batch.append(ast.literal_eval(message_queue.get()))

        batch_df = pd.DataFrame.from_records(batch)
        dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        batch_df["tz"] = dt

        # Find the highest 'price_level' for 'side' = 'bid'
        max_bid = find_extreme(batch_df, 'bid')
        # Find the lowest 'price_level' for 'side' = 'ask'
        min_ask = find_extreme(batch_df, 'ask')

        # Create new DataFrame by concatenating the selected rows
        if not max_bid.empty and not min_ask.empty:
            insights_df = pd.concat([max_bid, min_ask], axis=1).dropna().transpose()
            insights_df["difference"] = abs(min_ask['price_level'] - max_bid['price_level'])
            insights_df["mid_price"] = (min_ask['price_level'] + max_bid['price_level'])/2

            # insights_df.dropna(inplace=True)
            tz_max = insights_df['tz'].max()
            insights_df['tz'] = pd.to_datetime(insights_df['tz'])

            current = insights_df[insights_df['tz'] == tz_max]

            highest_bid = current.loc[insights_df['side'] == 'bid'].groupby(['product_id','tz'])['price_level'].agg('max').iloc[0]
            highest_bid_qty = current.loc[(insights_df['side'] == 'bid') & (current['price_level'] == highest_bid), 'quantity'].to_list()
            lowest_ask = current.loc[insights_df['side'] == 'ask'].groupby(['product_id','tz'])['price_level'].agg('min').iloc[0]
            lowest_ask_qty = current.loc[(insights_df['side'] == 'ask') & (current['price_level'] == lowest_ask), 'quantity'].to_list()

            highest_diff = insights_df['difference'].max()
            insights_df.set_index('tz', inplace=True)
            avg_mid_price_1m = insights_df["mid_price"].resample('1T').mean().mean()
            avg_mid_price_5m = insights_df["mid_price"].resample('5T').mean().mean()
            avg_mid_price_15m = insights_df["mid_price"].resample('15T').mean().mean()

            insights_df["highest_diff"] = highest_diff
            insights_df["highest_bid"] = highest_bid
            insights_df["highest_bid_qty"] = ','.join(map(str, highest_bid_qty))
            insights_df["lowest_ask"] = lowest_ask
            insights_df["lowest_ask_qty"] = ','.join(map(str, lowest_ask_qty))
            insights_df["avg_mid_price_1m"] = avg_mid_price_1m
            insights_df["avg_mid_price_5m"] = avg_mid_price_5m
            insights_df["avg_mid_price_15m"] = avg_mid_price_15m
            insights_df.drop(columns=['price_level', 'quantity', 'side','difference','mid_price'], inplace=True)
            insights_df.drop_duplicates(inplace=True)
            ### Uncomment to see in console what is saved to data.csv file
            print(f'Batch at {dt} insights: \n {tabulate(insights_df, headers="keys", tablefmt="psql")} \n')

            # Check if file already exists
            # file_exists = os.path.isfile('data.csv')
            # Write to file
            # insights_df.to_csv('data.csv', mode='a', index=False, header=not file_exists, float_format='%.10f')
        else:
            continue

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
