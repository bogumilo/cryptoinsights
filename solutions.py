import sys
from websocket import create_connection
import json
import time
import queue
import threading
import ast
from tabulate import tabulate
import pandas as pd
import subprocess

subprocess.Popen(['python', 'download.py'])
time.sleep(20)

# Queue to hold messages
message_queue = queue.Queue()

def transform_solution(solution):
    solution_str = str(solution)
    result = json.loads(solution_str)
    result_df = pd.DataFrame(result)
    result_df.dropna(inplace=True)
    tz_max = result_df['tz'].max()
    result_df['tz'] = pd.to_datetime(result_df['tz'])

    current = result_df[result_df['tz'] == tz_max]

    highest_bid = current.loc[result_df['side'] == 'bid'].groupby(['product_id','tz'])['price_level'].agg('max').iloc[0]
    highest_bid_qty = current.loc[(result_df['side'] == 'bid') & (current['price_level'] == highest_bid), 'quantity'].to_list()
    lowest_ask = current.loc[result_df['side'] == 'ask'].groupby(['product_id','tz'])['price_level'].agg('min').iloc[0]
    lowest_ask_qty = current.loc[(result_df['side'] == 'ask') & (current['price_level'] == lowest_ask), 'quantity'].to_list()

    highest_diff = result_df['difference'].max()
    # print(f'Diff: {highest_diff} ====\n\n')
    # print(current)
    result_df.set_index('tz', inplace=True)
    avg_mid_price_1m = result_df["mid_price"].resample('1T').mean().mean()
    avg_mid_price_5m = result_df["mid_price"].resample('5T').mean().mean()
    avg_mid_price_15m = result_df["mid_price"].resample('15T').mean().mean()

    transformed_solution = json.dumps({
        "tz": tz_max,
        "highest_bid": highest_bid,
        "highest_bid_qty": ','.join(map(str, highest_bid_qty)),
        "lowest_ask": lowest_ask,
        "lowest_ask_qty": ','.join(map(str, lowest_ask_qty)),
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
        solution_df = pd.DataFrame.from_records(batch)
        # dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        dt = solution_df["tz"].max()
        # print(solution_df)
        solution_df = solution_df[solution_df['tz'] == dt]
        solution_df.set_index('tz', inplace=True)

        # solution_df["highest_bid"] = solution_df["highest_bid"].max()
        # solution_df["lowest_ask"] = solution_df["lowest_ask"].min()
        solution_df.drop_duplicates(inplace=True)

        print(f'Insights at {dt} are: \n {tabulate(solution_df, headers="keys", tablefmt="psql")} \n')


def main():
    '''Ask user for a crypto product_id they are interested in'''
    try:
        threading.Thread(target=process_batch_solutions, args=(5,)).start()

        while True:
            solution = pd.read_csv('data.csv').to_json(orient='records')
            transformed_solution = transform_solution(solution)
            if transformed_solution is not None:
                message_queue.put(transformed_solution)

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    try:
        main()
        while len(sys.argv) == 1:
            main()
    except KeyboardInterrupt:
        print('\nGoodbye!')
        sys.exit(0)
