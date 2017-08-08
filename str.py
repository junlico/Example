#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import gs_connect

def daily_search_term(service):
    #search term report

    #service.clear(sid, range_name)
    usecols = [
                "Campaign Name", "Ad Group Name",
                "Customer Search Term", "Keyword", "Match Type",
                "Impressions", "Clicks", "Orders placed within 1-week of a click",
                "Total Spend", "Product Sales within 1-week of a click"
    ]
    rename = {
            "Customer Search Term":"Search Term",
            "Orders placed within 1-week of a click":"Order",
            "Total Spend": "Ads Cost",
            "Product Sales within 1-week of a click":"Sales $"
    }
    curr = gs_connect.get_df(today_report, usecols=usecols, rename=rename)
    print("Load data from today's report")
    prev = gs_connect.get_df(previous_report, usecols=usecols, rename=rename)
    print("Load data from yesterday's report")
    curr = curr.groupby(["Campaign Name", "Ad Group Name", "Keyword", "Search Term", "Match Type"]).sum()
    prev = prev.groupby(["Campaign Name", "Ad Group Name", "Keyword", "Search Term", "Match Type"]).sum()
    df = curr.sub(prev, fill_value=0).reset_index()
    df = df[df["Impressions"] > 0]
    filter_black = False
    if filter_black:
        black_list = service.read_single_column(sid, black_range)
        bl = "|".join(black_list)
        df = df[~df["Search Term"].str.contains(bl)]
        #df = df[df["Impressions"] > 0 & (~df["Search Term"].str.contains(bl))]
    r =  df.values.tolist()
    print("Finish analyzing")
    service.write_range(advertisement, range_name, r)
    print("Finish upload")

def left(usecols,rename):
    prev_report = "0806.txt"
    prev = gs_connect.get_df(prev_report,usecols=usecols, rename=rename)
    prev = prev.set_index(["Campaign Name", "Ad Group Name", "Keyword", "Search Term", "Match Type"])
    print(prev)
    print("Load data from prev")

def right(usecols,rename):
    curr_report = "0807.txt"
    curr = gs_connect.get_df(curr_report, usecols=usecols, rename=rename)
    curr = curr.groupby(["Campaign Name", "Ad Group Name", "Keyword", "Search Term", "Match Type"])

if __name__ == "__main__":
    #service = gs_connect.gservice()
    #daily_search_term(service)
    #=CONCATENATE("""",B2,""":[""", C2, """, """, D2, """]" )
    usecols = [
                "Campaign Name", "Ad Group Name",
                "Customer Search Term", "Keyword", "Match Type",
                "Impressions", "Clicks", "Total Spend",
                "Same SKU units Ordered within 1-week of click", "Other SKU units Ordered within 1-week of click",
                "Same SKU units Product Sales within 1-week of click", "Other SKU units Product Sales within 1-week of click"
    ]
    rename = {
            "Campaign Name":"Campain",
            "Ad Group Name":"Ad Group

            ",
            "Customer Search Term":"Search Term",
            "Orders placed within 1-week of a click":"Order",
            "Total Spend": "Ads Cost",
            "Product Sales within 1-week of a click":"Sales $"
    }
    left(usecols, rename)
