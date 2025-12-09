import asyncio
import gc
import json
import os
import traceback

import pandas as pd

import poly_data.CONSTANTS as CONSTANTS
import poly_data.global_state as global_state
from poly_data.data_utils import get_order, get_position, set_position
from poly_data.trading_utils import (
    get_best_bid_ask_deets,
    get_buy_sell_amount,
    get_order_prices,
    round_down,
    round_up,
)
from trading import send_buy_order, send_sell_order

from .base import BaseStrategy


class MarketMakerStrategy(BaseStrategy):
    async def execute(self, market_id, market_data):
        async with self.get_lock(market_id):
            try:
                client = self.client
                row = market_data

                round_length = len(str(row['tick_size']).split(".")[1])
                params = global_state.params[row['param_type']]

                market_details = [
                    {'name': 'token1', 'token': row['token1'], 'answer': row['answer1']},
                    {'name': 'token2', 'token': row['token2'], 'answer': row['answer2']}
                ]
                print(f"\n\n{pd.Timestamp.utcnow().tz_localize(None)}: {row['question']}")

                pos_1 = get_position(row['token1'])['size']
                pos_2 = get_position(row['token2'])['size']

                amount_to_merge = min(pos_1, pos_2)

                if float(amount_to_merge) > CONSTANTS.MIN_MERGE_SIZE:
                    pos_1 = client.get_position(row['token1'])[0]
                    pos_2 = client.get_position(row['token2'])[0]
                    amount_to_merge = min(pos_1, pos_2)
                    scaled_amt = amount_to_merge / 10**6

                    if scaled_amt > CONSTANTS.MIN_MERGE_SIZE:
                        print(f"Position 1 is of size {pos_1} and Position 2 is of size {pos_2}. Merging positions")
                        client.merge_positions(amount_to_merge, market_id, row['neg_risk'] == 'TRUE')
                        set_position(row['token1'], 'SELL', scaled_amt, 0, 'merge')
                        set_position(row['token2'], 'SELL', scaled_amt, 0, 'merge')

                for detail in market_details:
                    token = int(detail['token'])
                    orders = get_order(token)

                    deets = get_best_bid_ask_deets(market_id, detail['name'], 100, 0.1)

                    if deets['best_bid'] is None or deets['best_ask'] is None or deets['best_bid_size'] is None or deets['best_ask_size'] is None:
                        deets = get_best_bid_ask_deets(market_id, detail['name'], 20, 0.1)

                    best_bid = deets['best_bid']
                    best_bid_size = deets['best_bid_size']
                    second_best_bid = deets['second_best_bid']
                    second_best_bid_size = deets['second_best_bid_size']
                    top_bid = deets['top_bid']
                    best_ask = deets['best_ask']
                    best_ask_size = deets['best_ask_size']
                    second_best_ask = deets['second_best_ask']
                    second_best_ask_size = deets['second_best_ask_size']
                    top_ask = deets['top_ask']

                    best_bid = round(best_bid, round_length)
                    best_ask = round(best_ask, round_length)

                    try:
                        overall_ratio = (deets['bid_sum_within_n_percent']) / (deets['ask_sum_within_n_percent'])
                    except:
                        overall_ratio = 0

                    try:
                        second_best_bid = round(second_best_bid, round_length)
                        second_best_ask = round(second_best_ask, round_length)
                    except:
                        pass

                    top_bid = round(top_bid, round_length)
                    top_ask = round(top_ask, round_length)

                    pos = get_position(token)
                    position = pos['size']
                    avgPrice = pos['avgPrice']

                    position = round_down(position, 2)

                    bid_price, ask_price = get_order_prices(
                        best_bid, best_bid_size, top_bid, best_ask,
                        best_ask_size, top_ask, avgPrice, row
                    )

                    bid_price = round(bid_price, round_length)
                    ask_price = round(ask_price, round_length)

                    mid_price = (top_bid + top_ask) / 2

                    print(f"\nFor {detail['answer']}. Orders: {orders} Position: {position}, "
                          f"avgPrice: {avgPrice}, Best Bid: {best_bid}, Best Ask: {best_ask}, "
                          f"Bid Price: {bid_price}, Ask Price: {ask_price}, Mid Price: {mid_price}")

                    other_token = global_state.REVERSE_TOKENS[str(token)]
                    other_position = get_position(other_token)['size']

                    buy_amount, sell_amount = get_buy_sell_amount(position, bid_price, row, other_position)

                    max_size = row.get('max_size', row['trade_size'])

                    order = {
                        "token": token,
                        "mid_price": mid_price,
                        "neg_risk": row['neg_risk'],
                        "max_spread": row['max_spread'],
                        'orders': orders,
                        'token_name': detail['name'],
                        'row': row
                    }

                    print(f"Position: {position}, Other Position: {other_position}, "
                          f"Trade Size: {row['trade_size']}, Max Size: {max_size}, "
                          f"buy_amount: {buy_amount}, sell_amount: {sell_amount}")

                    os.makedirs('positions/', exist_ok=True)
                    fname = 'positions/' + str(market_id) + '.json'

                    if sell_amount > 0:
                        if avgPrice == 0:
                            print("Avg Price is 0. Skipping")
                            continue

                        order['size'] = sell_amount
                        order['price'] = ask_price

                        n_deets = get_best_bid_ask_deets(market_id, detail['name'], 100, 0.1)

                        mid_price = round_up((n_deets['best_bid'] + n_deets['best_ask']) / 2, round_length)
                        spread = round(n_deets['best_ask'] - n_deets['best_bid'], 2)

                        pnl = (mid_price - avgPrice) / avgPrice * 100

                        print(f"Mid Price: {mid_price}, Spread: {spread}, PnL: {pnl}")

                        risk_details = {
                            'time': str(pd.Timestamp.utcnow().tz_localize(None)),
                            'question': row['question']
                        }

                        try:
                            ratio = (n_deets['bid_sum_within_n_percent']) / (n_deets['ask_sum_within_n_percent'])
                        except:
                            ratio = 0

                        pos_to_sell = sell_amount

                        if (pnl < params['stop_loss_threshold'] and spread <= params['spread_threshold']) or row['3_hour'] > params['volatility_threshold']:
                            risk_details['msg'] = (f"Selling {pos_to_sell} because spread is {spread} and pnl is {pnl} "
                                                  f"and ratio is {ratio} and 3 hour volatility is {row['3_hour']}")
                            print("Stop loss Triggered: ", risk_details['msg'])

                            order['size'] = pos_to_sell
                            order['price'] = n_deets['best_bid']

                            risk_details['sleep_till'] = str(pd.Timestamp.utcnow().tz_localize(None) +
                                                            pd.Timedelta(hours=params['sleep_period']))

                            print("Risking off")
                            send_sell_order(order)
                            client.cancel_all_market(market_id)

                            open(fname, 'w').write(json.dumps(risk_details))
                            continue

                    max_size = row.get('max_size', row['trade_size'])

                    if position < max_size and position < 250 and buy_amount > 0 and buy_amount >= row['min_size']:
                        sheet_value = row['best_bid']

                        if detail['name'] == 'token2':
                            sheet_value = 1 - row['best_ask']

                        sheet_value = round(sheet_value, round_length)
                        order['size'] = buy_amount
                        order['price'] = bid_price

                        price_change = abs(order['price'] - sheet_value)

                        send_buy = True

                        if os.path.isfile(fname):
                            risk_details = json.load(open(fname))

                            start_trading_at = pd.to_datetime(risk_details['sleep_till'])
                            current_time = pd.Timestamp.utcnow().tz_localize(None)

                            print(risk_details, current_time, start_trading_at)
                            if current_time < start_trading_at:
                                send_buy = False
                                print(f"Not sending a buy order because recently risked off. "
                                     f"Risked off at {risk_details['time']}")

                        if send_buy:
                            if row['3_hour'] > params['volatility_threshold'] or price_change >= 0.05:
                                print(f'3 Hour Volatility of {row["3_hour"]} is greater than max volatility of '
                                      f'{params["volatility_threshold"]} or price of {order["price"]} is outside '
                                      f'0.05 of {sheet_value}. Cancelling all orders')
                                client.cancel_all_asset(order['token'])
                            else:
                                rev_token = global_state.REVERSE_TOKENS[str(token)]
                                rev_pos = get_position(rev_token)

                                if rev_pos['size'] > row['min_size']:
                                    print("Bypassing creation of new buy order because there is a reverse position")
                                    if orders['buy']['size'] > CONSTANTS.MIN_MERGE_SIZE:
                                        print("Cancelling buy orders because there is a reverse position")
                                        client.cancel_all_asset(order['token'])

                                    continue

                                if overall_ratio < 0:
                                    send_buy = False
                                    print(f"Not sending a buy order because overall ratio is {overall_ratio}")
                                    client.cancel_all_asset(order['token'])
                                else:
                                    if best_bid > orders['buy']['price']:
                                        print(f"Sending Buy Order for {token} because better price. "
                                              f"Orders look like this: {orders['buy']}. Best Bid: {best_bid}")
                                        send_buy_order(order)
                                    elif position + orders['buy']['size'] < 0.95 * max_size:
                                        print(f"Sending Buy Order for {token} because not enough position + size")
                                        send_buy_order(order)
                                    elif orders['buy']['size'] > order['size'] * 1.01:
                                        print(f"Resending buy orders because open orders are too large")
                                        send_buy_order(order)

                    elif sell_amount > 0:
                        order['size'] = sell_amount

                        tp_price = round_up(avgPrice + (avgPrice * params['take_profit_threshold']/100), round_length)
                        order['price'] = round_up(tp_price if ask_price < tp_price else ask_price, round_length)

                        tp_price = float(tp_price)
                        order_price = float(orders['sell']['price'])

                        diff = abs(order_price - tp_price)/tp_price * 100

                        if diff > 2:
                            print(f"Sending Sell Order for {token} because better current order price of "
                                  f"{order_price} is deviant from the tp_price of {tp_price} and diff is {diff}")
                            send_sell_order(order)
                        elif orders['sell']['size'] < position * 0.97:
                            print(f"Sending Sell Order for {token} because not enough sell size. "
                                  f"Position: {position}, Sell Size: {orders['sell']['size']}")
                            send_sell_order(order)

            except Exception as ex:
                print(f"Error performing trade for {market_id}: {ex}")
                traceback.print_exc()

            gc.collect()
            await asyncio.sleep(2)
