import time
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase, Decimal, OrderType, TradeType
from hummingbot.smart_components.executors.position_executor.position_executor import PositionExecutor
from hummingbot.smart_components.strategy_frameworks.data_types import OrderLevel, TripleBarrierConf
from hummingbot.smart_components.strategy_frameworks.directional_trading.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from tmc_lib.ta_util import TAUtil


class DanTrendMaV1Config(DirectionalTradingControllerConfigBase):
    strategy_name: str = "dan_trend_ma_v1"
    sma1_length:int = Field(default=25, ge=2, le=500)
    sma2_length:int = Field(default=50, ge=2, le=500)
    sma3_length:int = Field(default=100, ge=2, le=500)
    angle_length:int = Field(default=3, ge=3, le=100)
    std_span: Optional[int] = None


class DanTrendMaV1(DirectionalTradingControllerBase):
    min_tick_bars: int = 100
    def __init__(self, config: DanTrendMaV1Config):
        super().__init__(config)
        self.config = config
        max_length = max(config.sma1_length, config.sma2_length, config.sma3_length)
        self.min_tick_bars = max_length * config.angle_length

    def early_stop_condition(self, executor: PositionExecutor, order_level: OrderLevel) -> bool:
        """
        If an executor has an active position, should we close it based on a condition.
        """
        # tick_df = self.get_processed_data()
        # if len(tick_df) > self.min_tick_bars:
        #     return (
        #             (tick_df['sma2_angle'].iloc[-1] > 10 and tick_df['sma3_angle'].iloc[-1] > 20 and tick_df['sma1_angle'].iloc[-1] < 0) or
        #             (tick_df['sma2_angle'].iloc[-1] < -20 and tick_df['sma3_angle'].iloc[-1] < -10 and tick_df['sma1_angle'].iloc[-1] > 0)
        #     )
        return False

    def cooldown_condition(self, executor: PositionExecutor, order_level: OrderLevel) -> bool:
        """
        After finishing an order, the executor will be in cooldown for a certain amount of time.
        This prevents the executor from creating a new order immediately after finishing one and execute a lot
        of orders in a short period of time from the same side.
        """
        if executor.close_timestamp and executor.close_timestamp + order_level.cooldown_time > time.time():
            return True
        return False

    def get_processed_data(self) -> pd.DataFrame:
        ticks_df = self.candles[0].ticks_df
        ticks_df["signal"] = 0
        if len(ticks_df) > self.min_tick_bars:
            ticks_df.ta.sma(length=self.config.sma1_length, append=True)
            ticks_df.ta.sma(length=self.config.sma2_length, append=True)
            ticks_df.ta.sma(length=self.config.sma3_length, append=True)
            ticks_df['sma1_angle'] = TAUtil.generate_angle_pd_df(ticks_df[f"SMA_{self.config.sma1_length}"], self.config.angle_length)
            ticks_df['sma2_angle'] = TAUtil.generate_angle_pd_df(ticks_df[f"SMA_{self.config.sma2_length}"], self.config.angle_length)
            ticks_df['sma3_angle'] = TAUtil.generate_angle_pd_df(ticks_df[f"SMA_{self.config.sma3_length}"], self.config.angle_length)

            # Generate signal
            long_condition = (
                    (ticks_df[f"SMA_{self.config.sma1_length}"] > ticks_df[f"SMA_{self.config.sma2_length}"]) &
                    (ticks_df[f"SMA_{self.config.sma2_length}"] > ticks_df[f"SMA_{self.config.sma3_length}"]) &
                    (ticks_df['sma1_angle'] > 15) &
                    (ticks_df['sma2_angle'] > 15) &
                    (ticks_df['sma3_angle'] > 15)
            )

            short_condition = (
                    (ticks_df[f"SMA_{self.config.sma1_length}"] < ticks_df[f"SMA_{self.config.sma2_length}"]) &
                    (ticks_df[f"SMA_{self.config.sma2_length}"] < ticks_df[f"SMA_{self.config.sma3_length}"]) &
                    (ticks_df['sma1_angle'] < -15) &
                    (ticks_df['sma2_angle'] < -15) &
                    (ticks_df['sma3_angle'] < -15)
            )

            if long_condition.iloc[-1]:
                ticks_df.loc[long_condition, "signal"] = 1
            if short_condition.iloc[-1]:
                ticks_df.loc[short_condition, "signal"] = -1

        return ticks_df

    def to_format_status(self) -> list:
        lines = super().to_format_status()
        df = self.get_processed_data()
        lines.extend([f"total tick bars: {len(df)}. Minimum of {self.min_tick_bars}. Tick size: {self.candles[0].tick_size}"])
        return lines

    def extra_columns_to_show(self):
        lines = ["n_trades"]
        tick_df = self.get_processed_data()
        if len(tick_df) > self.min_tick_bars:
            lines += [
                # f"SMA_{self.config.sma1_length}",
                # f"SMA_{self.config.sma2_length}",
                # f"SMA_{self.config.sma3_length}",
                "sma1_angle",
                "sma2_angle",
                "sma3_angle",
            ]

        return lines
        # return [f"BBP_{self.config.bb_length}_{self.config.bb_std}",
        #         f"MACDh_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}",
        #         f"MACD_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}"]
