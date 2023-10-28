import unittest
from collections import deque

import pandas as pd
from decimal import Decimal
from hummingbot.tmc_lib.util_tick_bar import TickBarUtil


class TestTickBar(unittest.TestCase):
    def setUp(self):
        self.data = pd.read_csv('tmc_lib/tests/test_data_trades.csv', parse_dates=['time'])

    # def test_prepare_trades_df(self):
    #     df = pd.DataFrame({
    #         'timestamp': ['2021-01-01 00:00:00', '2021-01-01 00:01:00'],
    #         'amount': [1, 2],
    #         'value': [100, 200]
    #     })
    #     properties = ['timestamp', 'amount', 'value']
    #     prepared_df = TickBarUtil.prepare_trades_df(df, properties)
    #     expected_columns = ['time', 'qty', 'price']
    #     self.assertListEqual(list(prepared_df.columns), expected_columns)
    #
    # from decimal import Decimal

    # def test_tick_bar(self):
    #     processed_df = TickBarUtil.prepare_trades_df(self.data,['time','volume','price'])
    #     tick_bars = TickBarUtil.trades_to_tick_bars(processed_df, 10)
    #     expected_output = pd.DataFrame({
    #         'time': [processed_df['time'].iloc[0], processed_df['time'].iloc[10]],
    #         'open': [Decimal(200), Decimal(190)],
    #         'high': [Decimal(250), Decimal(300)],
    #         'low': [Decimal(150), Decimal(180)],
    #         'close': [Decimal(190), Decimal(300)],
    #         'volume': [Decimal(253000), Decimal(195000)]
    #     })
    #     pd.testing.assert_frame_equal(tick_bars, expected_output)

    def test_tick_bar_stream(self):
        tick_bars = deque(maxlen=10)
        for _, row in self.data.iterrows():
            trade = row.to_dict()
            tick_bars = TickBarUtil.trade_add_to_tick_bars(trade,tick_bars,5,{'timestamp':'time','price':'price','qty':'qty','side':'side','symbol':'symbol'})
        self.assertEqual(len(tick_bars), 4)
        first_bar = tick_bars[0]
        self.assertEqual(first_bar[1],200) # open
        self.assertEqual(first_bar[2],250) # high
        self.assertEqual(first_bar[3],150) # low
        self.assertEqual(first_bar[4],203) # close
        self.assertEqual(first_bar[5],800) # volume
        self.assertEqual(first_bar[6],154300) # quote_asset_volume
        self.assertEqual(first_bar[7],5) # trade count
        self.assertEqual(first_bar[8],400) # taker_buy_base_volume
        self.assertEqual(first_bar[9],90300) # taker_buy_quote_volume

        second_bar = tick_bars[1]
        self.assertEqual(second_bar[1],100) # open
        self.assertEqual(second_bar[2],190) # high
        self.assertEqual(second_bar[3],50) # low
        self.assertEqual(second_bar[4],190) # close
        self.assertEqual(second_bar[5],500) # volume
        self.assertEqual(second_bar[6],67000) # quote_asset_volume
        self.assertEqual(second_bar[7],5) # trade count
        self.assertEqual(second_bar[8],200) # taker_buy_base_volume
        self.assertEqual(second_bar[9],28000) # taker_buy_quote_volume

        third_bar = tick_bars[2]
        self.assertEqual(third_bar[1],190) # open
        self.assertEqual(third_bar[2],190) # high
        self.assertEqual(third_bar[3],150) # low
        self.assertEqual(third_bar[4],150) # close
        self.assertEqual(third_bar[5],700) # volume
        self.assertEqual(third_bar[6],115000) # volume
        self.assertEqual(third_bar[7],5) # trade count
        self.assertEqual(third_bar[8],500) # taker_buy_base_volume
        self.assertEqual(third_bar[9],81000) # taker_buy_quote_volume

        fourth_bar = tick_bars[3]
        self.assertEqual(fourth_bar[1],180) # open
        self.assertEqual(fourth_bar[2],250) # high
        self.assertEqual(fourth_bar[3],100) # low
        self.assertEqual(fourth_bar[4],250) # close
        self.assertEqual(fourth_bar[5],400) # close
        self.assertEqual(fourth_bar[6],71000) # close
        self.assertEqual(fourth_bar[7],5) # trade count
        self.assertEqual(fourth_bar[8],150) # taker_buy_base_volume
        self.assertEqual(fourth_bar[9],30500) # taker_buy_quote_volume


    def test_convert_data_to_trade_1(self):
        trade = {
            't': '2021-01-01 00:00:00',
            'p': 100,
            'q': 1,
            's': 'buy',
            'S': 'BTC-USDT'
        }
        properties = {
            'timestamp': 't',
            'price': 'p',
            'qty': 'q',
            'side': 's',
            'symbol': 'S'
        }
        result = TickBarUtil.convert_data_to_trade(trade, properties)
        self.assertEqual(result.timestamp, int(pd.to_datetime(trade['t']).timestamp()))
        self.assertEqual(result.price, trade['p'])
        self.assertEqual(result.qty, trade['q'])
        self.assertEqual(result.trade_type, trade['s'])
        self.assertEqual(result.symbol, trade['S'])

    def test_convert_data_to_trade_2(self):
        trade = {
            't': 1698345275,
            'p': 100,
            'q': 1,
            'm': True,
            'S': 'BTC-USDT'
        }
        properties = {
            'timestamp': 't',
            'price': 'p',
            'qty': 'q',
            'maker': 'm',
            'symbol': 'S'
        }
        result = TickBarUtil.convert_data_to_trade(trade, properties)
        self.assertEqual(result.timestamp, 1698345275)
        self.assertEqual(result.price, trade['p'])
        self.assertEqual(result.qty, trade['q'])
        self.assertEqual(result.trade_type, "buy")
        self.assertEqual(result.symbol, trade['S'])

if __name__ == '__main__':
    unittest.main()
