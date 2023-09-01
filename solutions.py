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

# Queue to hold messages
message_queue = queue.Queue()

def transform_solution(solution):
    solution_str = str(solution)
    result = json.loads(solution_str)
    result_df = pd.DataFrame(result)
    result_df.dropna(inplace=True)
    result_df['tz'] = pd.to_datetime(result_df['tz'])
    result_df.set_index('tz', inplace=True)

    # tz = result_df['tz']
    highest_bid = result_df.loc[result_df['side'] == 'bid', 'price_level'].max()
    highest_bid_qty = result_df.loc[(result_df['side'] == 'bid') & (result_df['price_level'] == highest_bid), 'quantity'].tolist()
    lowest_ask = result_df.loc[result_df['side'] == 'ask', 'price_level'].min()
    lowest_ask_qty = result_df.loc[(result_df['side'] == 'ask') & (result_df['price_level'] == lowest_ask), 'quantity'].tolist()

    highest_diff = result_df['difference'].max()

    avg_mid_price_1m = result_df["mid_price"].resample('1T').mean().mean()
    avg_mid_price_5m = result_df["mid_price"].resample('5T').mean().mean()
    avg_mid_price_15m = result_df["mid_price"].resample('15T').mean().mean()

    transformed_solution = json.dumps({
        # "tz": dt,
        "highest_bid": highest_bid,
        "highest_bid_qty": ', '.join(map(str, highest_bid_qty)),
        "lowest_ask": lowest_ask,
        "lowest_ask_qty": ', '.join(map(str, lowest_ask_qty)),
        "highest_diff": highest_diff,
        "avg_mid_price_1m": avg_mid_price_1m,
        "avg_mid_price_5m": avg_mid_price_5m,
        "avg_mid_price_15m": avg_mid_price_15m
    })

    return transformed_solution.encode("utf-8").decode("utf-8")

def process_batch_solutions(interval):
    '''Process insights every interval seconds'''
    while True:
        batch = [] # your batch
        start_time = time.time()
        while time.time() - start_time < interval:
            if not message_queue.empty():
                batch.append(ast.literal_eval(message_queue.get()))
        dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        # print(f'{batch} \n')

        solution_df = pd.DataFrame.from_records(batch)
        # solution_df['tz'] = dt
        solution_df.drop_duplicates(inplace=True)
        # solution_df.set_index('tz', inplace=True)

        print(f'Insights at {dt} are: \n {tabulate(solution_df, headers="keys", tablefmt="psql")} \n')


def main():
    '''Ask user for a crypto product_id they are interested in'''
    threading.Thread(target=process_batch_solutions, args=(5,)).start()

    while True:
        solution = pd.read_csv('data.csv').to_json(orient='records')
        transformed_solution = transform_solution(solution)
        if transformed_solution is not None:
            message_queue.put(transformed_solution)

if __name__ == '__main__':
    try:
        main()
        while len(sys.argv) == 1:
            main()
    except KeyboardInterrupt:
        print('\nGoodbye!')
        sys.exit(0)
