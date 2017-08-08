#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pandas as pd
from get_data import gservice

def file_dir(file_name):
    home_dir = os.path.expanduser("~")
    return os.path.join(home_dir, "Downloads", file_name)

def read_storage_report(file_name):
    df = pd.read_csv(file_dir(file_name), sep="\t", encoding="latin1")
    item_info = {}
    for row in df.values:
        if row[0] not in item_info:
            size_tier = row[13]
            item_volume = row[11]
            longest_side = row[5]
            median_side = row[6]
            shortest_side = row[7]
            item_weight = row[9]
            item_info[row[0]] = [size_tier, longest_side, median_side, shortest_side, item_volume, item_weight]
    return item_info

def upload_fee_preview(service, file_name):
    usecols = [
        "asin",
        "longest-side", "median-side", "shortest-side", "item-package-weight",
        "sales-price", "estimated-referral-fee-per-unit", "expected-fulfillment-fee-per-unit"
    ]

    df = pd.read_csv(file_dir(file_name), sep="\t", usecols=usecols)
    df = df[["asin",
             "longest-side", "median-side", "shortest-side","item-package-weight",
             "sales-price", "estimated-referral-fee-per-unit", "expected-fulfillment-fee-per-unit"]]
    lists = df.values.tolist()
    
    service.write_range(sid, range_name, lists)

def upload_report(ASIN_list):
    uploads =[]
    file_name = "storage_fee.txt"
    report_info = read_storage_report(file_name)
    for ASIN in ASIN_list:
        if ASIN[0] in report_info:
            uploads.append(report_info[ASIN[0]])
        else:
            uploads.append([])
    return uploads

if __name__ == "__main__":
    service = gservice()
    upload_fee_preview(service, "fee_preview.txt")
