#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gs_connect           #gs_connect.py
import pandas as pd

def load_data(file_list):
    data_list = []
    usecols=["asin","quantity"]
    for f in file_list:
        df = gs_connect.get_df(f, usecols)
        data_list.append(df)
        print("Finish %s  %s" % (f, df.shape[0]))
    frame = pd.concat(data_list)
    df = frame.groupby("asin").sum()
    print(df)
    return df


if __name__ == "__main__":

    sales_list = ["mar_sales.txt",
                 "apr_sales.txt",
                 "may_sales.txt",
                 "jun_sales.txt",
                 "jul_sales.txt"]
    return_list = ["return.txt"]
    usecols = ["asin","quantity"]
    sales_data = gs_connect.get_df(sales_list, usecols = ["asin","quantity"]).groupby("asin").sum()
    return_data = gs_connect.get_df(return_list, usecols = ["asin","quantity"]).groupby("asin").sum()

    lists = sales_data.sub(return_data, fill_value=0).reset_index().values.tolist()

    
    service = gs_connect.gservice()
    service.write_range(sid, range_name, lists)
