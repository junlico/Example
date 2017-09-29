#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, re
import gs_connect
import pandas
import datetime

download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
advertisement_sid = "1QUnqXtCyuqwghnYam1uzTvIQuIaVl3PbG2wO1rvFZ_I"

def get_search_term_report_df(file_name):
    use_cols = [
        "Campaign Name", "Ad Group Name",
        "Customer Search Term", "Keyword", "Match Type", "First Day of Impression", "Last Day of Impression",
        "Impressions", "Clicks", "Total Spend",
        "Same SKU units Ordered within 1-week of click", "Other SKU units Ordered within 1-week of click",
        "Same SKU units Product Sales within 1-week of click", "Other SKU units Product Sales within 1-week of click"
    ]

    rename = {
        "Customer Search Term":"Search Term",
        "First Day of Impression":"From",
        "Last Day of Impression":"To",
        "Average CPC":"CPC",
        "Total Spend": "Ads Cost",
        "Same SKU units Product Sales within 1-week of click":"Same Sales",
        "Same SKU units Ordered within 1-week of click":"Same Order",
        "Other SKU units Ordered within 1-week of click":"Other Order",
        "Other SKU units Product Sales within 1-week of click":"Other Sales"
    }

    df = pandas.read_csv(gs_connect.file_in_downloads(file_name), sep="\t", usecols=use_cols)
    df = df.rename(columns=rename)
    return df

def get_weekly_df(report_list):
    def get_date(file_name):
        try:
            return datetime.datetime(*map(int, [file_name[19:23], file_name[24:26], file_name[27:29]]))
        except IndexError:
            print("File name not in correct formatting...")

    curr_report = report_list[0]
    curr_date = get_date(curr_report)
    #get previous one week report.
    prev_report, prev_date = next((report, get_date(report)) for report in report_list[1:] if (curr_date - get_date(report))>= datetime.timedelta(days=7))

    curr_df = get_search_term_report_df(curr_report)
    prev_df = get_search_term_report_df(prev_report)
    monthly_df = curr_df.copy()

    curr_date = curr_df["To"].max()
    prev_date = prev_df["To"].max()

    index = ["Campaign Name", "Ad Group Name", "Keyword", "Match Type", "Search Term", "From"]
    curr_df = curr_df.loc[curr_df["To"] == curr_date].set_index(index)
    prev_df = prev_df.loc[prev_df["To"] == prev_date].set_index(index)

    del curr_df["To"]
    del prev_df["To"]
    from_to_df = curr_df.sub(prev_df).dropna().reset_index()
    from_to_df = from_to_df.loc[from_to_df["Impressions"] > 0]

    try:
        from_to_df["From"] = prev_date
        from_to_df["To"] = curr_date
        print(curr_date, from_to_df.shape[0])
        return calculate_df(monthly_df), calculate_df(from_to_df)
    except Error:
        print("Empty DataFrame")

def calculate_df(df):
    df.loc[df["Clicks"] <= 0,"CPC"] = 0
    df.loc[df["Clicks"] > 0, "CPC"] = (df["Ads Cost"] / df["Clicks"]).round(2)
    df.loc[(df["Same Sales"] + df["Other Sales"]) <= 0,"ACoS"] = 0
    df.loc[(df["Same Sales"] + df["Other Sales"]) > 0, "ACoS"] = (df["Ads Cost"] / (df["Same Sales"] + df["Other Sales"])).round(2)
    reorder = ["Campaign Name", "Ad Group Name", "Keyword", "Match Type", "Search Term", "From", "To",
            "Impressions", "Clicks", "CPC", "ACoS",
            "Same Order", "Same Sales", "Other Order", "Other Sales"
    ]
    return df.loc[:, reorder]

def upload_ads(service):
    ''' get data from search-term-report.txt '''
    file_list = os.listdir(download_dir)
    pattern = r"search-term-report-\d{4}-\d{2}-\d{2}-\d+.txt"
    selected_list = sorted([f for f in file_list if re.match(pattern, f)], reverse=True)
    monthly_df, weekly_df = get_weekly_df(selected_list)

    service.delete_range(advertisement_sid, "Weekly")
    service.delete_range(advertisement_sid, "Monthly")
    weekly_values = [weekly_df.columns.tolist()] + weekly_df.values.tolist()
    monthly_values = [monthly_df.columns.tolist()] + monthly_df.values.tolist()
    print("Uploading weekly reports...")
    service.write_range(advertisement_sid, "Weekly!A:O", weekly_values)
    print("Uploading monthly reports...")
    service.write_range(advertisement_sid, "Monthly!A:O", monthly_values)

if __name__ == "__main__":
    service = gs_connect.gservice()
    upload_ads(service)
