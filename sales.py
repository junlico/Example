# -*- coding: utf-8 -*-
import pandas as pd
import gs_connect
import datetime


def sales_order():
	sales_list = ["mar_sales.txt", "apr_sales.txt", "may_sales.txt", "jun_sales.txt",
	    "jul_sales.txt", "aug_sales.txt"]
	use_cols = ["amazon-order-id", "sku", "quantity-shipped", "item-price", "tracking-number"]
	rename = {
	    "amazon-order-id":"order-id",
		"quantity-shipped":"quantity"
	}
	sales_df = gs_connect.get_df(sales_list, usecols=use_cols, rename=rename)
	sales_df = sales_df.drop_duplicates(subset=["order-id", "sku", "tracking-number"])
	sales_df["unit-price"] = sales_df["item-price"] / sales_df["quantity"]
	del sales_df["tracking-number"]
	del sales_df["item-price"]
	return sales_df.set_index(["order-id", "sku"])


def commision_order(service, sid):
	commision_range = "Commision!L:M"
	values = service.read_range(sid, commision_range)
	commision_df = pd.DataFrame(values[1:], columns=values[0])
	commision_df = commision_df.loc[~commision_df["order-id"].isnull()]
	index = ["order-id", "sku"]
	df = commision_df.set_index(index)
	return df


def return_order():
	use_cols = ["order-id", "sku", "quantity", "license-plate-number"]
	return_df = gs_connect.get_df("return.txt", usecols=use_cols)
	return_df = return_df.drop_duplicates(subset=["order-id", "sku", "license-plate-number"])
	del return_df["license-plate-number"]
	#return_df = return_df.groupby(["order-id", "sku"]).sum()
	#print(return_df)
	return return_df


def calculate(service):
	pass


if __name__ == "__main__":
	#sales_df = sales_order()
	'''
	service = gs_connect.gservice()
	commision_sid = "1_2UrwkHleTTz35heCb72LEaQL0o-ing0XbiaxKZjW7s"
	commision_df = commision_order(service, commision_sid)
	commision_df = commision_df.loc[commision_df.index.isin(sales_df.index)]
	sales_df = sales_df.loc[~sales_df.index.isin(commision_df.index)]
	'''
	sales_df = sales_order()
	return_df = return_order().reset_index()
	df = return_df.loc[return_df["order-id"] == "113-8150078-7369849"]
	print(df)
	'''
	sales_df = sales_df.loc[sales_df.index.isin(return_df.index)]
	sales_df = sales_df.reset_index()
	return_df = return_df.reset_index()
	#print(sales_df)
	#print(return_df)
	df = pd.merge(sales_df, return_df, how="left", on=["order-id", "sku"])
	print(df)
	'''
