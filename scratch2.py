import numpy as np
import pandas as pd

foo = 5 * np.ones((5, 5))
foo[:2, :] = np.nan
foo[2, 0] = np.nan
foo[2, -1] = np.nan

bar = 10 * np.ones((5, 5))

