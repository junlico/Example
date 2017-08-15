#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
import gs_connect
import datetime
from itertools import cycle

def get_cols():
    use_cols = [
        "Campaign Name", "Ad Group Name",
        "Customer Search Term", "Keyword", "Match Type", "First Day of Impression", "Last Day of Impression",
        "Impressions", "Clicks", "Total Spend",
        "Same SKU units Ordered within 1-week of click", "Other SKU units Ordered within 1-week of click",
        "Same SKU units Product Sales within 1-week of click", "Other SKU units Product Sales within 1-week of click"
    ]
    rename = {
        "Customer Search Term":"Search Term",
        "Same SKU units Ordered within 1-week of click":"Same Order",
        "Other SKU units Ordered within 1-week of click":"Other Order",
        "First Day of Impression":"From",
        "Last Day of Impression":"To",
        "Total Spend": "Ads Cost $",
        "Same SKU units Product Sales within 1-week of click":"Same Sales",
        "Other SKU units Product Sales within 1-week of click":"Other Sales"
    }
    return use_cols, rename


def sid_list():
    ads0 = "1cM4VK53zdHfnVco6-kdsdAkpI0wT9WoasM0xfQ8eSGw"
    ads1 = "1tPht2bHKrfNrmZQaRgwNCq5jZOviXjfhMP6UhgGsnCY"
    ads2 = "1Rj0uyjs39xs3PMY-AVtWjpVpsBmcA29vM7NZq_gRhFI"
    ads3 = "1V9B5VPI1oy-BVpYmePvlkYClFLEL36yI70M2KusSQpA"
    ads4 = "1K-2wZdAHOicggNFZ_rcATN2EgX9SEl6THCGmS___6Xo"
    ads5 = "1pepR2k-6hkYRuA5TaHRPOd0IjE2pqXuzwTlfbG1skFM"
    return [ads0, ads1, ads2, ads3, ads4, ads5]


def get_campaign(service, sid):
    range_name = "Campaign!A2:A"
    return service.read_single_column(sid, range_name)


def get_negative_keyword(service, sid):
    range_name = "Negative Keywords!B:E"
    values = service.read_range(sid, range_name)
    neg_keyword_df = pd.DataFrame(values[1:], columns=values[0])
    neg_keyword_df = neg_keyword_df.set_index(["Campaign", "Group", "Match Type"])
    return neg_keyword_df

def remove_negative_keyword(data_df, negative_keyword_df):
    df = data_df.loc[data_df["Campaign Name"].isin(negative_keyword_df["Campaign"])]
    print(df.shape[0])

def reorder():
    return ["Campaign Name", "Ad Group Name", "Keyword", "Match Type", "Search Term", "From", "To",
            "Impressions", "Clicks", "CTR", "Ads Cost $", "CPC", "ACoS",
            "Same Order", "Same Sales", "Other Order", "Other Sales"
    ]


def polish_df(df):
    df.loc[df["Impressions"] <= 0,"CTR"] = 0
    df.loc[df["Impressions"] > 0, "CTR"] = df["Clicks"] / df["Impressions"]
    df.loc[df["Clicks"] <= 0,"CPC"] = 0
    df.loc[df["Clicks"] > 0, "CPC"] = df["Ads Cost $"] / df["Clicks"]
    df.loc[(df["Same Sales"] + df["Other Sales"]) <= 0,"ACoS"] = 0
    df.loc[(df["Same Sales"] + df["Other Sales"]) > 0, "ACoS"] = df["Ads Cost $"] / (df["Same Sales"] + df["Other Sales"])
    df = df[reorder()]
    return df


def latest_df(date):
    report = date.strftime("%m%d") + ".txt"
    end = date.strftime("%m/%d/%Y")
    use_cols, rename = get_cols()
    df = gs_connect.get_df(report, usecols=use_cols, rename=rename)
    end_df = df.loc[df["To"] == end]
    if end_df.empty:
        end = (date - datetime.timedelta(days=1)).strftime("%m/%d/%Y")
        end_df = df.loc[df["To"] == end]
    return end_df, end

def latest_report(curr):
    date = datetime.datetime.now() - datetime.timedelta(days=curr)
    report = date.strftime("%m%d") + ".txt"
    use_cols, rename = get_cols()
    use_cols += ["CTR", "Average CPC", "ACoS"]
    return gs_connect.get_df(report, usecols=use_cols, rename=rename)


def diff_df(curr, prev):
    end = datetime.datetime.now() - datetime.timedelta(days=curr)
    begin = datetime.datetime.now() - datetime.timedelta(days=prev)
    index = ["Campaign Name", "Ad Group Name", "Keyword", "Match Type", "Search Term", "From"]
    end_df, end = latest_df(end)
    begin_df, begin = latest_df(begin)
    #DataFrame : From 'end' to 'end'
    curr_df = end_df.loc[end_df["From"] == end]
    end_df = end_df.set_index(index)
    begin_df = begin_df.set_index(index)
    del end_df["To"]
    del begin_df["To"]
    begin_end_df = end_df.sub(begin_df).dropna()
    if not begin_end_df.empty:
        begin_end_df = begin_end_df.reset_index()
        begin_end_df["From"] = begin
        begin_end_df["To"] = end
        df = polish_df(pd.concat([begin_end_df, curr_df]))
        print("    Finish read report %s : %d" %(end, df.shape[0]))
        return df
    elif not curr_df.empty:
        print("    Finish read report %s : %d" %(end, curr_df.shape[0]))
        return polish_df(curr_df)

def push_ads(service, data_df, month_df):
    ads = [0, 1, 2, 3, 5, 6]
    data_range = "Data!A2:Q"
    month_range = "Month!A2:Q"
    sids = sid_list()
    for sid in sids:
        index = sids.index(sid)
        campaign = service.read_single_column(sid, "Campaign!A2:A")
        print(campaign)
        if not campaign:
            print("    No campaign list in Ads%d" % ads[index])
            continue
        else:
            service.clear(sid, data_range)
            df = data_df.loc[data_df["Campaign Name"].isin(campaign)]
            #remove_negative_keyword(df, get_negative_keyword(service, sid))
            service.write_range(sid, data_range, df.values.tolist())
            service.write_range(sid, month_range, month_df.values.tolist())
            print("    Finish push data to Ads%d" % ads[index])


def uploading(service):
    dates = [1,2,4,5,6,7,8]
    #date_list = list(range(1,8))
    data_list = []
    for curr, prev in zip(dates, dates[1:]):
        df = diff_df(curr, prev)
        data_list.append(df)
    data_df = pd.concat(data_list)
    month_df = latest_report(dates[0])
    push_ads(service, data_df, month_df)

if __name__ == "__main__":
    print("Connecting Google Service...")
    service = gs_connect.gservice()
    uploading(service)
