from typing import Union
import numpy
import numpy as np
from collections import deque
import pandas as pd
from collections.abc import Iterable
from pandas import DataFrame


class TAUtil:

    @staticmethod
    def calculate_angle(data: Union[Iterable, DataFrame, numpy.ndarray], length: int = 3) -> float:
        if len(data) < length:
            return 0

        if isinstance(data, deque):
            y = list(data)[-length:]
        elif isinstance(data, DataFrame):
            y = data.tail(length).values.flatten()
        elif isinstance(data, numpy.ndarray):
            y = data[-length:]
        else:
            raise TypeError(f"Data must be of type deque, DataFrame, or numpy.ndarray, received {type(data)}")

        x = list(range(1, length + 1))
        slope, _ = np.polyfit(x, y, 1)
        angle_rad = np.arctan(slope)
        angle_deg = np.degrees(angle_rad)
        return angle_deg

    @staticmethod
    def generate_angle_pd_df(data: Union[Iterable, DataFrame], length: int = 3) -> pd.DataFrame:
        # passed data example:
        #   [1, 1.2679491924311228, 1.5358983848622457,       1.823,      2.58923]
        # angles pd data expected:
        #   [0,                  0,                 15, 15.51068597,  27.77424267]
        angles = [np.nan] * (length - 1)
        for i in range(length, len(data) + 1):
            subset = data[i-length:i]
            if isinstance(subset, pd.DataFrame):
                subset = subset.values.flatten()
            angle = TAUtil.calculate_angle(subset, length)
            angles.append(angle)
        return pd.DataFrame(angles)
