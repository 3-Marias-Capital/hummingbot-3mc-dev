from decimal import Decimal

import pandas as pd
import pandas_ta as ta

from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig, CandlesFactory
from hummingbot.strategy.directional_strategy_base import DirectionalStrategyBase


class RSI(DirectionalStrategyBase):
    """
    RSI (Relative Strength Index) strategy implementation based on the DirectionalStrategyBase.

    This strategy uses the RSI indicator to generate trading signals and execute trades based on the RSI values.
    It defines the specific parameters and configurations for the RSI strategy.

    Parameters:
        directional_strategy_name (str): The name of the strategy.
        trading_pair (str): The trading pair to be traded.
        exchange (str): The exchange to be used for trading.
        order_amount_usd (Decimal): The amount of the order in USD.
        leverage (int): The leverage to be used for trading.

    Position Parameters:
        stop_loss (float): The stop-loss percentage for the position.
        take_profit (float): The take-profit percentage for the position.
        time_limit (int): The time limit for the position in seconds.
        trailing_stop_activation_delta (float): The activation delta for the trailing stop.
        trailing_stop_trailing_delta (float): The trailing delta for the trailing stop.

    Candlestick Configuration:
        candles (List[CandlesBase]): The list of candlesticks used for generating signals.

    Markets:
        A dictionary specifying the markets and trading pairs for the strategy.

    Methods:
        get_signal(): Generates the trading signal based on the RSI indicator.
        get_processed_df(): Retrieves the processed dataframe with RSI values.
        market_data_extra_info(): Provides additional information about the market data.

    Inherits from:
        DirectionalStrategyBase: Base class for creating directional strategies using the PositionExecutor.
    """
    directional_strategy_name: str = "RSI"
    # Define the trading pair and exchange that we want to use and the csv where we are going to store the entries
    trading_pair: str = "ETH-USDT"
    exchange: str = "binance_perpetual"
    order_amount_usd = Decimal("40")
    leverage = 10

    # Configure the parameters for the position
    stop_loss: float = 0.0075
    take_profit: float = 0.015
    time_limit: int = 60 * 1
    trailing_stop_activation_delta = 0.004
    trailing_stop_trailing_delta = 0.001
    cooldown_after_execution = 10

    candles = [CandlesFactory.get_candle(CandlesConfig(connector=exchange, trading_pair=trading_pair, interval="30m", max_records=1000, tick_size=150))]
    markets = {exchange: {trading_pair}}

    tick_df: pd.DataFrame = pd.DataFrame()
    rsi_df: pd.DataFrame = pd.DataFrame()

    def get_signal(self):
        """
        Generates the trading signal based on the RSI indicator.
        Returns:
            int: The trading signal (-1 for sell, 0 for hold, 1 for buy).
        """

        # Do Nothing
        return 0

        candles_df = self.get_processed_df()
        current_rsi = self.rsi_df.tail(1).values.flatten()[0] if len(self.rsi_df) > 0 else 0
        if current_rsi > 70:
            return -1
        elif current_rsi < 30:
            return 1
        else:
            return 0



    def get_processed_df(self):
        """
        Retrieves the processed dataframe with RSI values.
        Returns:
            pd.DataFrame: The processed dataframe with RSI values.
        """
        # candles_df = self.candles[0].candles_df
        # candles_df.ta.rsi(length=7, append=True)

        tick_df = self.candles[0].ticks_df
        if len(tick_df) > 7:
            tick_df.ta.rsi(length=7, append=True)
            # self.rsi_df = ta.rsi(tick_df["close"],7)

        return tick_df

    def market_data_extra_info(self):
        """
        Provides additional information about the market data to the format status.
        Returns:
            List[str]: A list of formatted strings containing market data information.
        """
        lines = []
        candles_df = self.get_processed_df()
        if len(candles_df) > 7:
            columns_to_show = ["timestamp", "open", "low", "high", "close", "volume", "RSI_7"]
        else:
            columns_to_show = ["timestamp", "open", "low", "high", "close", "volume"]
        lines.extend([f"Candles: {self.candles[0].name} | Interval: {self.candles[0].interval}\n"])
        lines.extend(self.candles_formatted_list(candles_df, columns_to_show))
        last_tick_bar = self.candles[0].ticks_df.tail(1)
        tick_count = len(self.candles[0].ticks_df)
        trades = last_tick_bar['n_trades'].values.flatten()[0]
        open = last_tick_bar['open'].values.flatten()[0]
        high = last_tick_bar['high'].values.flatten()[0]
        low = last_tick_bar['low'].values.flatten()[0]
        close = last_tick_bar['close'].values.flatten()[0]
        volume = last_tick_bar['volume'].values.flatten()[0]
        # current_rsi = self.rsi_df.tail(1).values.flatten()[0] if len(self.rsi_df) > 0 else 0
        lines.extend([f"Ticks:{tick_count} trades: {trades} open:{open} high:{high} low:{low} close:{close} volume:{volume}\n"])
        lines.extend([f"==================================================================================================="])
        return lines
