from New_Data_requests import NewData
import pandas as pd
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
if __name__ == '__main__':
    data = NewData()
    #data.first_run('2024-01-01')
    data.not_first_run()

