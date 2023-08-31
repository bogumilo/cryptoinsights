import sys
import websocket, json

SOCKET_URI = "wss://ws-feed-public.sandbox.exchange.coinbase.com"

subscribe_message = {
        "type": "subscribe",
        "product_ids": [
            'BTC-USD'
        ],
        "channels": ["level2_batch"]
    }

def on_open(ws):
    print("websocket opened")
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    result = json.loads(message)
    if 'type' in result and result['type'] == 'l2update' and 'product_id' in result and result['changes'][0][2] != '0.00000000':
        # Extract data from the websocket message
        time = result['time']
        product_id = result['product_id']
        side = ['ask' if result['changes'][0][0] == 'sell' else 'bid'][0]
        price_level = float(result['changes'][0][1])
        quantity = float(result['changes'][0][2])

        # Generate new message in format
        transformed_message = json.dumps({
            "time": time,
            "product_id": product_id,
            "side": side,
            "price_level": price_level,
            "quantity": quantity,
        })

        # future = publisher.publish(topic_path, data, origin="websocket-sample")
        print(transformed_message.encode("utf-8").decode("utf-8"))


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
        ws = websocket.WebSocketApp(SOCKET_URI, on_open=on_open, on_message=on_message)

        print(f"Here are insights for {product_id}:")
        ws.run_forever()

if __name__ == '__main__':
    try:
        main()
        while len(sys.argv) == 1:
            main()
    except KeyboardInterrupt:
        print('\nGoodbye!')
        sys.exit(0)
