import sys
from websocket import create_connection
import json
import time
import queue
import threading
import ast
import pandas as pd

SOCKET_URI = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

subscribe_message = {
        "type": "subscribe",
        "product_ids": [
            'BTC-USD'
        ],
        "channels": ["level2_batch"]
    }

# Queue to hold messages
message_queue = queue.Queue()

def transform_message(message):
    result = json.loads(message)
    if 'type' in result and result['type'] == 'l2update' and 'product_id' in result and result['changes'][0][2] != '0.00000000':
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

def process_batch_messages(interval):
    while True:
        batch = [] # your batch
        start_time = time.time()
        while time.time() - start_time < interval:
            if not message_queue.empty():
                batch.append(ast.literal_eval(message_queue.get()))

        batch_df = pd.DataFrame.from_records(batch)
        # Find the highest 'quantity' for 'side' = 'bid'
        max_bid = batch_df.loc[batch_df[batch_df['side'] == 'bid']['price_level'].idxmax()]
        # Find the lowest 'quantity' for 'side' = 'ask'
        min_ask = batch_df.loc[batch_df[batch_df['side'] == 'ask']['price_level'].idxmin()]

        # Create new DataFrame by concatenating the selected rows
        insights_df = pd.concat([max_bid, min_ask], axis=1).transpose()
        insights_df["difference"] = min_ask['price_level'] - max_bid['price_level']
        insights_df["mid_price"] = (min_ask['price_level'] + max_bid['price_level'])/2

        dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        print(f'Batch at {dt} insights: \n {insights_df} \n')

def main():
    '''Ask user for a crypto product_id they are interested in'''
    if len(sys.argv) > 1:
        query = str(sys.argv[1])
    else:
        query = input("Provide crypto product to generate insights in format e.g. BTC-USD :\n> ")
    product_id = query.upper()
    if not product_id == 'BTC-USD':
        print(f"Error: Current websocket doesn't support '{query.upper()}' product. Try again")
    else:
        # Start the function in a separate thread
        threading.Thread(target=process_batch_messages, args=(5,)).start()

        ws = create_connection(SOCKET_URI)
        print("websocket opened")
        ws.send(json.dumps(subscribe_message))

        while True:
            message = ws.recv()
            transformed_message = transform_message(message)
            if transformed_message is not None:
                message_queue.put(transformed_message)

if __name__ == '__main__':
    try:
        main()
        while len(sys.argv) == 1:
            main()
    except KeyboardInterrupt:
        print('\nGoodbye!')
        sys.exit(0)
