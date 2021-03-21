from dark.tdresearch import get_iown
from dark.data import run_tda
from dark.analysis import get_pool
from dark.funcs import *
from time import time
t1 = time()
# Step 1
run_tda()

# Step 2
get_iown(return_sorted_df(r'E:\Github\dpool\bin\dfc.csv'))

# Step 3
get_pool()

print("Run Time = ", round(time() - t1))
