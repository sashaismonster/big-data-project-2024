import os
import pandas as pd


def preprocess(data):
    columns = [
        "q_unix_time",
        "q_read_time",
        "q_date",
        "q_time_h",
        "underlying_last",
        "expire_date",
        "expire_unix", 
        "dte",
        "c_delta",
        "c_gamma",
        "c_vega",
        "c_theta",
        "c_rho",
        "c_iv",
        "c_volume",
        "c_last",
        "c_size",
        "c_bid",
        "c_ask",
        "strike",
        "p_bid",
        "p_ask",
        "p_size",
        "p_last",
        "p_delta",
        "p_gamma",
        "p_vega",
        "p_theta",
        "p_rho",
        "p_iv",
        "p_volume",
        "strike_distance",
        "strike_distance_pct"
    ]
    full_data = pd.DataFrame(columns=columns)
    for chunk in data:
        chunk = chunk[chunk.notna()]
        chunk = chunk[chunk.notnull()]
        chunk.columns = columns
        full_data = pd.concat([full_data, chunk], axis=0, join='inner', copy=False)

    remained_columns = [
        "q_unix_time",
        "q_time_h",
        "underlying_last",
        "expire_unix", 
        "dte",
        "c_volume",
        "c_last",
        "c_size",
        "c_bid",
        "c_ask",
        "strike",
        "p_bid",
        "p_ask",
        "p_size",
        "p_last",
        "p_volume",
    ]
    full_data = full_data[remained_columns]
    numerics = [x for x in remained_columns if x not in ['c_size', 'p_size']]
    full_data[numerics] = full_data[numerics].apply(pd.to_numeric, errors='coerce')
    full_data.dropna(inplace=True)
    return {'data': full_data}


if __name__ == '__main__':
    files = preprocess(pd.read_csv('../data/data.csv', chunksize=10**5, low_memory=False, na_values=['', ' ', '\n']))
    for file_name, df in files.items():
        df.to_csv('%s.csv' % os.path.join('../data', file_name), chunksize=10**5)
    
    
