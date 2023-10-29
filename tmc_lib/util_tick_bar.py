import datetime
from collections import deque

import numpy as np
import pandas as pd
from typing import Any, Dict, Optional
from decimal import Decimal

from pandas import Timestamp

from tmc_lib.trade_type import TradeType


class TickBarUtil:
    @staticmethod
    def prepare_trades_df(df: pd.DataFrame, properties: list) -> pd.DataFrame:
        """
        Prepares a DataFrame for conversion to tick bars.

        :param df: DataFrame with columns as specified in properties.
        :param properties: List of column names in the order ['time', 'volume', 'price'].
        :return: DataFrame with columns renamed to 'time', 'volume', and 'price'.
        """
        df = df.rename(columns={properties[0]: 'time', properties[1]: 'qty', properties[2]: 'price'})

        return df

    @staticmethod
    def trades_to_tick_bars(trades_df: pd.DataFrame, tick_size: int) -> pd.DataFrame:
        """
        Converts a DataFrame of trades to tick bars.

        :param trades_df: DataFrame with columns 'time', 'volume', and 'price'.
        :param tick_size: The number of trades per bar.
        :return: DataFrame with 'time' as datetime, 'open' as Decimal, 'high' as Decimal, 'low' as Decimal, 'close' as Decimal, and 'volume' as Decimal.
        """
        if tick_size <= 0:
            raise ValueError("tick_size must be greater than 0")

        required_columns = ['time', 'qty', 'price']
        if not all(column in trades_df.columns for column in required_columns):
            raise ValueError(f"Input DataFrame is missing one or more required columns: {required_columns}")
        
        # Convert 'time' column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(trades_df['time']):
            trades_df['time'] = pd.to_datetime(trades_df['time'])
        
        # Convert 'volume' and 'price' columns to Decimal
        trades_df['qty'] = trades_df['qty'].apply(Decimal)
        trades_df['price'] = trades_df['price'].apply(Decimal)
        
        # Create a new DataFrame with 'time', 'open', 'high', 'low', 'close', 'volume' columns
        tick_bars_df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

        # Populate the new DataFrame from the trades_df
        for i in range(0, len(trades_df), tick_size):
            chunk = trades_df.iloc[i:i+tick_size]
            time = chunk['time'].iloc[0]
            open_price = chunk['price'].iloc[0]
            high_price = chunk['price'].max()
            low_price = chunk['price'].min()
            close_price = chunk['price'].iloc[-1]
            volume = (chunk['price'] * chunk['qty']).sum()
            tick_bars_df = pd.concat([tick_bars_df, pd.DataFrame([{'time': time, 'open': open_price, 'high': high_price, 'low': low_price, 'close': close_price, 'volume': volume}])], ignore_index=True)
        
        return tick_bars_df

    @staticmethod
    def trades_to_tick_bars_2(trades_df: pd.DataFrame, tick_size: int, properties: Dict[str, str], collection_size: int = 1000) -> pd.DataFrame:
        """
        Converts a DataFrame of trades to tick bars using the trade_add_to_tick_bars method.

        :param trades_df: DataFrame with columns 'time', 'volume', and 'price'.
        :param tick_size: The number of trades per bar.
        :param properties: Dictionary with keys 'time', 'qty', 'price', and 'side' or 'maker'.
        :return: DataFrame with 'time' as datetime, 'open' as Decimal, 'high' as Decimal, 'low' as Decimal, 'close' as Decimal, and 'volume' as Decimal.
        """
        # Initialize an empty DataFrame
        ticks_df = deque(maxlen=collection_size)

        # For each trade in trades_df, call trade_add_to_tick_bars
        for _, trade in trades_df.iterrows():
            ticks_df = TickBarUtil.trade_add_to_tick_bars(trade.to_dict(), ticks_df, tick_size, properties)

        return ticks_df

    @staticmethod
    def trade_add_to_tick_bars(trade: Dict[str, Any], ticks_df: pd.DataFrame, tick_size: int, properties: Dict[str, str], time_as_int: bool = True) -> pd.DataFrame:
        trade_obj = TickBarUtil.convert_data_to_trade(trade,properties)

        volume = trade_obj.qty
        quote_asset_volume = trade_obj.price * trade_obj.qty
        taker_buy_base_volume = trade_obj.qty if trade_obj.trade_type == 'buy' else 0
        taker_buy_quote_volume = quote_asset_volume if trade_obj.trade_type == 'buy' else 0
        init_trade_count = 1
        open = trade_obj.price
        high = trade_obj.price
        low = trade_obj.price
        close = trade_obj.price

        # time_dt = trade_obj.timestamp if time_as_int else trade_obj.datetime

        if not ticks_df:
            ticks_df.append(np.array([trade_obj.timestamp, open, high, low, close, volume,
                                     quote_asset_volume, init_trade_count, taker_buy_base_volume,
                                     taker_buy_quote_volume]))
        else:
            last_tick_bar = ticks_df[-1]
            if int(last_tick_bar[7]) < tick_size:
                last_tick_bar[2] = max(last_tick_bar[2], trade_obj.price)  # update high
                last_tick_bar[3] = min(last_tick_bar[3], trade_obj.price)  # update low
                last_tick_bar[4] = trade_obj.price # close
                last_tick_bar[5] += volume  # update volume
                last_tick_bar[6] += quote_asset_volume  # update quote_asset_volume
                last_tick_bar[7] += 1  # n_trades + 1
                last_tick_bar[8] += taker_buy_base_volume  # update taker_buy_base_volume
                last_tick_bar[9] += taker_buy_quote_volume  # update taker_buy_quote_volume
            else:
                ticks_df.append(np.array([trade_obj.timestamp, open, high, low, close, volume,
                                          quote_asset_volume, init_trade_count, taker_buy_base_volume,
                                          taker_buy_quote_volume]))
        return ticks_df

    @staticmethod
    def convert_data_to_trade(trade: Dict[str, Any], properties: Dict[str, str]) -> TradeType:
        # CHECK TIMESTAMP
        datetime_f = ""
        if 'timestamp' not in properties:
            raise KeyError("properties dictionary must contain a 'timestamp' key")
        time_data = trade.get(properties['timestamp'])
        if not time_data:
            raise ValueError("'time_data' cannot be empty or non-existent")
        if isinstance(time_data, int):
            timestamp = time_data
        elif isinstance(time_data, Timestamp):
            timestamp = time_data.timestamp()
        elif isinstance(time_data, str):
            # If time_data is a string that can be parsed into a datetime, do so
            try:
                datetime_info = pd.to_datetime(time_data)
                timestamp = int(datetime_info.timestamp())
            except ValueError:
                raise ValueError("'time_data' must be a timestamp or a string that can be parsed into a datetime")
        else:
            raise TypeError(f"'time_data' of type {type(time_data).__name__} must be an integer (timestamp) or a string")

        # dt_object = datetime.datetime.fromtimestamp(timestamp)
        # datetime_f = dt_object.isoformat()
        # if datetime_f == "":
        #     raise ValueError("datetime_f is not set")

        # CHECK PRICE
        if 'price' not in properties:
            raise KeyError("properties dictionary must contain a 'price' key")
        price_data = trade.get(properties['price'])
        if price_data is None or price_data == '':
            raise ValueError("'price_data' cannot be empty or non-existent")

        try:
            price_data = float(price_data)
        except ValueError:
            raise TypeError("'price_data' must be a numeric value")

        # CHECK QTY
        if 'qty' not in properties:
            raise KeyError("properties dictionary must contain a 'qty' key")
        qty_data = trade.get(properties['qty'])
        if qty_data is None or qty_data == '':
            raise ValueError("'qty_data' cannot be empty or non-existent")
        try:
            qty_data = float(qty_data)
        except ValueError:
            raise TypeError("'qty_data' must be a numeric value")

        # CHECK IF IT IS A BUY
        trade_type = ""
        if 'side' in properties:
            side_data = trade.get(properties['side']).lower()
            if side_data not in ['buy', 'sell']:
                raise ValueError("'side_data' must be either 'buy' or 'sell'")
            trade_type = side_data

        if trade_type == "" and 'maker' in properties:
            maker_data = trade.get(properties['maker'])
            if not isinstance(maker_data,bool):
                raise TypeError("'maker' must be a bool")
            trade_type = "buy" if maker_data else "sell"

        if trade_type == "":
            raise ValueError("trade type not determined")

        # CHECK SYMBOL
        if 'symbol' not in properties:
            raise KeyError("properties dictionary must contain a 'symbol' key")
        symbol_data = trade.get(properties['symbol'])
        if not isinstance(symbol_data,str):
            raise TypeError("'symbol_data' must be a string")

        trade = TradeType(
            timestamp=timestamp,
            datetime=datetime_f,
            price=price_data,
            qty=qty_data,
            trade_type=trade_type,
            symbol=symbol_data
        )
        return trade
