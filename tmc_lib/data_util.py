import pandas as pd
import numpy as np

# This class is a collection of static methods to process market data
class DataUtil:
    @staticmethod
    def ohlc_resample_data(ohlcv_data: pd.DataFrame, time_interval: str):
        """
        it receives a pd df of ohlcv candles of 1m. then it converts it into a
        different timeframe. For example a collection of 1m for 1 day is received, and time_interval
        received is 30m, it then returns a resampled df of 30m ohlcv candles.
        The resampled DataFrame is then returned.
        """
        if not isinstance(time_interval, str):
            raise TypeError("The 'time_interval' must be a string.")
        if ohlcv_data.empty:
            return ohlcv_data
        try:
            ohlcv_data.set_index('origin_time', inplace=True)
            ohlcv_data.index = pd.to_datetime(ohlcv_data.index)
        except ValueError:
            raise TypeError("The 'origin_time' column must contain datetime values.")

        # Do not fill missing values

        # Use the simple aggregation functions
        def first_non_nan(x):
            return x.dropna().iloc[0] if not x.dropna().empty else np.nan

        def last_non_nan(x):
            return x.dropna().iloc[-1] if not x.dropna().empty else np.nan

        ohlc_dict = {
            'open': first_non_nan,
            'high':'max',
            'low':'min',
            'close': last_non_nan
        }

        # Ensure that the 'open', 'high', 'low', and 'close' columns contain numeric data
        numeric_columns = ohlcv_data.select_dtypes(include=[np.number]).columns
        if set(['open', 'high', 'low', 'close']).issubset(numeric_columns):
            ohlcv_data = ohlcv_data[numeric_columns].resample(time_interval).agg(ohlc_dict)
        else:
            raise TypeError("Non-numeric data found in the 'open', 'high', 'low', or 'close' columns.")
        return ohlcv_data

    @staticmethod
    def trades_to_dollar_bars(trades_df: pd.DataFrame, dollar_size: str):
        """
        This method receives a DataFrame of trades and a dollar size. It then converts the trades into dollar bars
        based on the given dollar size. For example, if the dollar size is '1M', it groups trades into bars where
        the total value of trades in each bar is at least 1 million dollars. It returns a DataFrame of dollar bars
        with 'open', 'high', 'low', 'close', and 'volume' columns.
        """
        # Convert dollar bar size to integer
        dollar_size = int(dollar_size.replace('k', '000').replace('M', '000000'))
        # Sort the trades DataFrame by index in ascending order
        trades_df = trades_df.sort_index()
        # Initialize an empty DataFrame for the dollar bars
        dollar_bars = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        # Initialize variables for the open, high, close prices, volume and the dollar value
        open_price = high_price = close_price = dollar_value = volume = 0
        # Initialize low_price to a very large number
        low_price = float('inf')
        # Iterate over the trade data
        for index, row in trades_df.iterrows():
            # Skip rows with missing values
            if pd.isna(row['price']) or pd.isna(row['quantity']):
                continue
            # If this is the first trade in the dollar bar
            if dollar_value == 0:
                open_price = row['price']
            # Add the dollar value of the trade to the running total
            dollar_value += row['price'] * row['quantity']
            # Add the quantity to the volume
            volume += row['quantity']
            # If the running total has reached the dollar bar size
            if dollar_value >= dollar_size:
                # The close price is the price of the current trade
                close_price = row['price']
                # Calculate the high and low prices based on the open and close prices
                high_price = max(open_price, close_price)
                low_price = min(open_price, close_price)
                # Add the dollar bar to the DataFrame
                new_bar = pd.DataFrame({'open': [open_price], 'high': [high_price], 'low': [low_price], 'close': [close_price], 'volume': [volume]})
                dollar_bars = pd.concat([dollar_bars, new_bar], ignore_index=True)
                # Reset the running total, volume and start a new dollar bar
                dollar_value = volume = 0
        return dollar_bars

    @staticmethod
    def time_bars_to_dollar_bars(ohlc_df: pd.DataFrame, dollar_size: str):
        """
        This method receives a DataFrame of 1m candlesticks and a dollar size. It then converts the 1m candlestick bars into dollar bars
        based on the given dollar size. For example, if the dollar size is '1M', it groups 1m candles into bars where
        the total value of trades in each bar is at least 1 million dollars. It returns a DataFrame of dollar bars
        with 'open', 'high', 'low', 'close', and 'volume' columns.
        """
        # Convert dollar bar size to integer
        dollar_size = int(dollar_size.replace('k', '000').replace('M', '000000'))
        # Sort the DataFrame by 'origin_time' in ascending order
        ohlc_df = ohlc_df.sort_values('origin_time')
        # Initialize an empty DataFrame for the dollar bars
        dollar_bars = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        # Initialize variables for the open, high, close prices, volume and the dollar value
        open_price = high_price = close_price = dollar_value = volume = 0
        # Initialize low_price to a very large number
        low_price = float('inf')
        # Iterate over the 1m candlestick bars
        for index, row in ohlc_df.iterrows():
            # If this is the first bar in the dollar bar
            if dollar_value == 0:
                open_price = row['open']
            # Add the dollar value of the bar to the running total
            dollar_value += row['close'] * row['volume']
            # Add the volume of the bar to the total volume
            volume += row['volume']
            # If the running total has reached the dollar bar size
            if dollar_value >= dollar_size:
                # The close price is the price of the current bar
                close_price = row['close']
                # Calculate the high and low prices based on the open and close prices
                high_price = max(open_price, close_price)
                low_price = min(open_price, close_price)
                # Add the dollar bar to the DataFrame
                new_bar = pd.DataFrame({'open': [open_price], 'high': [high_price], 'low': [low_price], 'close': [close_price], 'volume': [volume]}, index=[index])
                dollar_bars = pd.concat([dollar_bars, new_bar])
                # Reset the running total, volume and start a new dollar bar
                dollar_value = volume = 0
                open_price = high_price = close_price = dollar_value = volume = 0
                low_price = float('inf')
        return dollar_bars

    @staticmethod
    def trades_to_tick_bars(trades_df: pd.DataFrame, tick_size: int):
        """
        This method receives a DataFrame of trades and a tick size. It then converts the trades into tick bars
        based on the given tick size. For example, if the tick size is 10, it groups trades into bars where
        the total number of trades in each bar is at least 10. It returns a DataFrame of tick bars
        with 'open', 'high', 'low', 'close', and 'volume' columns.
        """
        # Sort the trades DataFrame by index in ascending order
        trades_df = trades_df.sort_index()
        # Initialize an empty DataFrame for the tick bars
        tick_bars = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        # Initialize variables for the open, high, close prices, and volume
        open_price = high_price = close_price = volume = 0
        # Initialize low_price to a very large number
        low_price = float('inf')
        # Initialize a counter for the number of trades
        trade_count = 0
        # Iterate over the trade data
        for index, row in trades_df.iterrows():
            # Skip rows with missing values
            if pd.isna(row['price']) or pd.isna(row['quantity']):
                continue
            # If this is the first trade in the tick bar
            if trade_count == 0:
                open_price = row['price']
            # Add the quantity to the volume
            volume += row['quantity']
            # Update the high and low prices
            high_price = max(high_price, row['price'])
            low_price = min(low_price, row['price'])
            # Increment the trade count
            trade_count += 1
            # If the trade count has reached the tick size
            if trade_count == tick_size:
                # The close price is the price of the current trade
                close_price = row['price']
                # Add the tick bar to the DataFrame
                new_bar = pd.DataFrame({'open': [open_price], 'high': [high_price], 'low': [low_price], 'close': [close_price], 'volume': [volume]}, index=[index])
                tick_bars = pd.concat([tick_bars, new_bar])
                # Reset the trade count, volume and start a new tick bar
                trade_count = volume = 0
                open_price = high_price = close_price = 0
                low_price = float('inf')
        return tick_bars
