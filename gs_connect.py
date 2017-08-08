#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pandas as pd
import httplib2
import argparse
from apiclient.discovery import build
from oauth2client import client, tools
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials


def file_in_downloads(file_name):
    home_dir = os.path.expanduser("~")
    return os.path.join(home_dir, "Downloads", file_name)

def get_df(file_name, **key):
    if isinstance(file_name, str):
        if key["usecols"]:
            df = pd.read_csv(file_in_downloads(file_name), sep="\t", encoding="ISO-8859-1", usecols=key["usecols"])
        else:
            df = pd.read_csv(file_in_downloads(file_name), sep="\t", encoding="ISO-8859-1")
    elif isinstance(file_name, list):
        data_list = []
        for f in file_name:
            if "usecols" in key:
                df = pd.read_csv(file_in_downloads(f), sep="\t", header=0, encoding="ISO-8859-1", usecols=key["usecols"])
            else:
                df = pd.read_csv(file_in_downloads(f), sep="\t", header=0, encoding="ISO-8859-1")
            data_list.append(df)
            print("Finish %s %s" % (f, df.shape[0]))
        df = pd.concat(data_list)
    if "rename" in key:
        df = df.rename(columns=key["rename"])
    return df

class gservice(object):
    def __init__(self):
        self.flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        self.scopes = "https://www.googleapis.com/auth/spreadsheets"
        self.client_secret_file = "client_secret.json"
        self.connect()

    def get_credentials(self):
        store = Storage("client_sheet.json")
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(self.client_secret_file, self.scopes)
            flow.user_agent = "SpreadSheet"
            credentials = tools.run_flow(flow, store, self.flags)
        return credentials

    def connect(self):
        credentials = self.get_credentials()
        http = credentials.authorize(httplib2.Http())
        #discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?' 'version=v4')
        self.service = build('Sheets', 'v4', http=http)

    def clear(self, sid, range_name):
        request = self.service.spreadsheets().values().clear(spreadsheetId=sid, range=range_name, body={})
        response = request.execute()

    def read_single_column(self, sid, range_name):
        result = self.service.spreadsheets().values().get(spreadsheetId=sid, range=range_name, majorDimension="COLUMNS").execute()
        values = result.get("values", [])
        return values[0] if len(values) != 0 else values

    def read_range(self, sid, range_name):
        result = self.service.spreadsheets().values().get(spreadsheetId=sid, range=range_name).execute()
        values = result.get("values", [])
        return values

    def read_multiple_range(self, sid, range_name):
        #range is a list, better to have header to make DataFrame
        result = self.service.spreadsheets().values().batchGet(spreadsheetId=sid, ranges=range_name, majorDimension="COLUMNS").execute()
        #size = len(range_name)
        #temp = []
        #for i in range(size):
        values = result.get("valueRanges")[i].get("values")
        '''
            col_number = len(values)
            for col in range(col_number):
                temp.append(values[col])
        df = pandas.DataFrame({temp[x][0]: temp[x][1:] for x in range(size)})
        return df
        '''
        return values

    def write_range(self, sid, range_name, values):
        body = {'values': values}
        self.service.spreadsheets().values().update(spreadsheetId=sid, range=range_name, valueInputOption="RAW", body=body).execute()

    def write_multi_range(self, sid, range_name, values):
        '''
        #https://developers.google.com/sheets/api/guides/values
        values = [
            [
                #cells values
            ],
            #additional rows
        ]
        '''
        data = [{"range": range_name, "values": values}]
        body = {"valueInputOption": "RAW", "data": data}
        result = service.spreadsheets().values().batchUpdate(spreadsheetId=sid, body=body).execute()


if __name__ == "__main__":
    
