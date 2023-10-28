import unittest
from collections import deque

import numpy as np
import pandas as pd

from hummingbot.tmc_lib.ta_util import TAUtil

class TestTAUtil(unittest.TestCase):

    def test_angle_10(self):
        data = deque([6, 5, 4, 1, 1.1763, 1.3526])
        calculated_angle = TAUtil.calculate_angle(data)
        self.assertAlmostEqual(calculated_angle, 10, places=1)

    def test_angle_10_df(self):
        data = pd.DataFrame([6, 5, 4, 1, 1.1763, 1.3526])
        calculated_angle = TAUtil.calculate_angle(data)
        self.assertAlmostEqual(calculated_angle, 10, places=1)

    def test_angle_20(self):
        data = deque([6,5,4, 1, 1.3639, 1.7278])
        self.assertAlmostEqual(TAUtil.calculate_angle(data), 20, places=1)

    def test_angle_30(self):
        data = deque([4,5,6, 1, 1.5773, 2.1546])
        self.assertAlmostEqual(TAUtil.calculate_angle(data), 30, places=1)

    def test_angle_minus_15(self):
        data = deque([9,8,7, 1, 0.7321, 0.4642])
        self.assertAlmostEqual(TAUtil.calculate_angle(data), -15, places=1)

    def test_angle_minus_40(self):
        data = deque([1, 0.1609, -0.6782])
        self.assertAlmostEqual(TAUtil.calculate_angle(data), -40, places=1)

    def test_generate_angle_pd_df(self):
        data = pd.DataFrame([1, 1.2679491924311228, 1.5358983848622457, 1.823, 2.58923])
        expected_result = pd.DataFrame([np.nan, np.nan, 15, 15.51068597, 27.77424267])  # Adjusted expected result
        result = TAUtil.generate_angle_pd_df(data,3)
        pd.testing.assert_frame_equal(result, expected_result)

if __name__ == '__main__':
    unittest.main()
