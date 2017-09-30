#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, re
import gs_connect
import pandas
import datetime

download_dir = os.path.join(os.path.expanduser("~"), "Downloads")


inventory_sid = "1_3ajuJe5OTPCpzeqJfs_UIbS6q_40Ux9-don-2Kkssk"
report_sid = "1yCXhEcWtFPLQJG-Llfu4zUOSuqDrGsFF_qc5RA18WgY"
promotion_sid = "192n31mchT87xVPUvrdNoUZ_jo0EkS8zj4Levsi7MvkU"

def file_in_downloads(file_name):
    return os.path.join(download_dir, file_name)

def get_product_info_df(service):
    values = service.read_range(inventory_sid, "Inventory!A:M")
    use_cols = ["SID", "ASIN", "SKU", "FID", "Length", "Width", "Height", "Case Weight"]
    return pandas.DataFrame(values[1:], columns=values[0]).loc[:,use_cols].fillna("")

#get sales quantity list, ["ASIN", "Qty", "Qty", "Qty"....], use when amazon server is down.
def get_sales_quantity_list(sales_file_name):
    use_cols = ["purchase-date", "sales-channel", "order-status", "asin", "quantity"]
    sales_df = gs_connect.get_df(sales_file_name, usecols=use_cols)
    sales_df = sales_df.loc[(sales_df["sales-channel"] == "Amazon.com") & (sales_df["order-status"] != "Cancelled")]
    sales_df = sales_df.drop(["sales-channel", "order-status"], axis=1)
    sales_df["purchase-date"] = sales_df["purchase-date"].str[:10]
    sales_df = sales_df.groupby(["asin", "purchase-date"]).sum().reset_index()
    df = sales_df.pivot(index="asin", columns="purchase-date", values="quantity").fillna(0)
    #save data into csv file in Downloads folder
    df.to_csv(gs_connect.file_in_downloads("sales.csv"),float_format="%.f")

def update_transaction(service, file_name, product_info_df):
    print(file_name)
    use_cols = ["type", "order id", "sku", "description", "quantity", "product sales", "total"]
    transaction_df = pandas.read_csv(file_in_downloads(file_name), skiprows=7, usecols=use_cols, thousands=",")
    order_df = transaction_df.loc[transaction_df["type"]=="Order", ["sku", "quantity", "product sales", "total"]].groupby(["sku"], as_index=False).sum()
    refund_df = transaction_df.loc[transaction_df["type"]=="Refund", ["sku", "quantity", "product sales", "total"]].groupby(["sku"], as_index=False).sum()
    curr_df = pandas.merge(order_df, refund_df, how="left", on=["sku"]).fillna(0)
    #curr_df = pandas.merge(product_info_df.loc[:,["SID", "ASIN", "SKU"]], transaction_df, how="left", left_on=["SKU"], right_on=["sku"]).fillna(0)
    # print(curr_df.head(20))
    # print(curr_df.head(10))
    curr_df["Total Sales"] = curr_df["product sales_x"] + curr_df["product sales_y"]
    curr_df["Total Revenue"] = curr_df["total_x"] + curr_df["total_y"]
    curr_df = curr_df.loc[:, ["sku", "Total Sales", "Total Revenue", "quantity_x", "quantity_y"]].rename(columns={"sku":"SKU", "quantity_x":"Sold", "quantity_y":"Return"})
    # print(curr_df.head(20))
    # print(curr_df.dropna().shape[0])

    prev_updates = service.read_range(report_sid, "Transaction!C:G")
    prev_df = pandas.DataFrame(prev_updates[1:], columns=prev_updates[0])
    prev_df = prev_df.apply(pandas.to_numeric, errors="ignore")
    # print(prev_df.iloc[:, 1:].head(20))
    # print(prev_df.head(10))
    # print(prev_df.dtypes)

    update_df = pandas.concat([prev_df, curr_df]).groupby(["SKU"], as_index=False).sum()
    update_df = pandas.merge(product_info_df.loc[:, ["SID", "ASIN", "SKU"]], update_df, how="left", on=["SKU"]).fillna(0)
    print(update_df.head(20))
    update_values = [update_df.columns.tolist()] + update_df.values.tolist()
    #service.clear(report_sid, "Transaction!A:G")
    # print("Uploading transactions...")
    #service.write_range(report_sid, "Transaction!A:G", values)


def update_inventory(service):
    update = service.read_range(inventory_sid, "Update Inventory!B:D")
    shipment = service.read_range(inventory_sid, "Shipment!A:L")

    update_df = pandas.DataFrame(update[1:], columns=update[0])
    shipping_df = update_df.loc[(update_df["Type"] == "Shipping") & (pandas.isnull(update_df["Status"]))]
    shipment_history = pandas.DataFrame(shipment[1:], columns=shipment[0]).loc[:,["ID", "SKU", "Case"]]
    shipping_df = pandas.merge(shipping_df, shipment_history, how="left", on=["ID"]).loc[:,["SKU", "Case"]]
    shipping_df["Case"] = shipping_df["Case"].astype(str).astype(float)
    shipping_df = shipping_df.groupby("SKU").sum().reset_index()
    #print(shipping_df)

    inventory = service.read_range(inventory_sid, "Inventory!C:G")
    inventory_df = pandas.DataFrame(inventory[1:], columns=inventory[0]).loc[:, ["SKU", "Case #"]].fillna(0)
    df = pandas.merge(inventory_df, shipping_df, how="left", on=["SKU"])
    df["Case #"] = df["Case #"].astype(str).astype(float)
    df.loc[pandas.notnull(df["Case"]), "Case #"] = df["Case #"] - df["Case"]
    #print(df)
    case_df = df.loc[:,["Case #"]]
    values = case_df.values.tolist()
    print("Update Inventory...")
    service.write_range(inventory_sid, "Inventory!G2:G", values)
    #'''
    update_df["Status"] = "Update"
    #status = update_df.Status.tolist()
    status_df = update_df.loc[:, ["Status"]]
    status = status_df.values.tolist()
    print("Update Status...")
    service.write_range(inventory_sid, "Update Inventory!D2:D", status)

def test(service):
    promotion_order = service.read_range(promotion_sid, "刷单详情!E:L")
    promotion_df = pandas.DataFrame(promotion_order[1:], columns=promotion_order[0])
    promotion_df = promotion_df.loc[:, ["Order ID", "ASIN"]]
    promotion_df["Order ID"] = promotion_df["Order ID"].str.strip()
    promotion_df["ASIN"] = promotion_df["ASIN"].str.strip()
    use_cols = ["amazon-order-id", "purchase-date", "sales-channel", "order-status", "asin", "quantity", "item-price"]
    order_df = pandas.read_csv(gs_connect.file_in_downloads("sep_sales.txt"), sep="\t", usecols=use_cols)
    order_df = order_df.loc[(order_df["sales-channel"] == "Amazon.com") & (order_df["order-status"] != "Cancelled")]
    order_df["purchase-date"] = order_df["purchase-date"].str[:10]
    df = pandas.merge(promotion_df, order_df, how="left", left_on=["Order ID", "ASIN"], right_on=["amazon-order-id", "asin"])
    #df = df.loc[:, ["purchase-date"]].fillna("")
    purchase_date = df.values.tolist()
    service.write_range(promotion_sid, "刷单详情!A2:A", purchase_date)
    #print(df)

def update_fee_preview(service, file_name, product_info_df):
    use_cols = ["asin", "sales-price", "estimated-referral-fee-per-unit", "expected-fulfillment-fee-per-unit"]
    rename = {"estimated-referral-fee-per-unit":"referral", "expected-fulfillment-fee-per-unit":"fulfillment"}
    curr_df = pandas.read_csv(os.path.join(download_dir, file_name), sep="\t", encoding="ISO-8859-1", usecols=use_cols).rename(columns=rename)

    prev_updates = service.read_range(report_sid, "Product_Info!A:F")
    prev_df = pandas.DataFrame(prev_updates[1:], columns=prev_updates[0])
    prev_df = pandas.merge(product_info_df.loc[:, ["SID", "ASIN"]], prev_df, how="left", on=["SID", "ASIN"])
    upload_df = pandas.merge(prev_df, curr_df, how="left", left_on=["ASIN"], right_on=["asin"])
    upload_df.loc[pandas.notnull(upload_df["sales-price"]), "Current Price"] = upload_df["sales-price"]
    upload_df.loc[pandas.notnull(upload_df["fulfillment"]), "Fulfillment"] = upload_df["fulfillment"]
    upload_df.loc[pandas.notnull(upload_df["sales-price"]), "Referral %"] = (upload_df["referral"] / upload_df["sales-price"]).round(2)
    upload_df.loc[pandas.notnull(upload_df["sales-price"]), "Update Date"] = datetime.datetime.now().strftime("%m/%d/%Y")
    upload_df = upload_df.loc[:, "SID":"Update Date"].fillna("")
    service.clear(report_sid, "Product_Info!A:F")
    upload_values = [upload_df.columns.tolist()] + upload_df.values.tolist()
    print("Upload Fee Preview Info...")
    service.write_range(report_sid, "Product_Info!A:F", upload_values)


def read_report(service, product_info_df):
    #check_list for fee_preview_report, order_report, return_report, ads_cost_report
    file_list = os.listdir(download_dir)
    '''
    product_pattern = r"\d+.txt"
    check_list = [False, False]
    selected_list = sorted([f for f in file_list if re.match(product_pattern, f)], reverse=True)
    for f in selected_list:
        if all(check == True for check in check_list):
            break

        df = pandas.read_csv(os.path.join(download_dir, f), sep="\t", encoding="ISO-8859-1", nrows=0)
        if any(header == "longest-side" for header in df.columns.tolist()) and not check_list[0]:
            # if the report is fee_preview report, it contains "longest-side" in the DataFrame header
            update_fee_preview(service, f, product_info_df)
            check_list[0] = True


    '''
    #print(sales_df)
    prev_date = service.read_range(report_sid, "Transaction!N1")[0][0]
    if prev_date < datetime.datetime.now().strftime("%m/%d/%Y"):
        payment_pattern = r"\w+-\w+CustomTransaction.csv"
        selected_list = sorted([f for f in file_list if re.match(payment_pattern, f)], reverse=True)
        update_transaction(service, selected_list[0], product_info_df)
    #product_info_df.to_csv("SKU.csv", sep="\t", index=False)

if __name__ == "__main__":
    # service = gs_connect.gservice()
    # product_info_df = get_product_info_df(service)
    # read_report(service, product_info_df)
    s = "2017Mar1-2017Sep16CustomTransaction.csv"
    start_index = s.find("-")
    end_index = s.find("CustomTransaction")
    print(s[start_index + 1: end_index])
    start_date = datetime.datetime.strptime(s[:s.find("-")], "%Y%b%d").strftime("%m/%d/%Y")
    end_date = datetime.datetime.strptime(s[start_index + 1: end_index], "%Y%b%d").strftime("%m/%d/%Y")
    print(start_date, end_date)
    print(end_date+1)
    # print(s.find("-"))
    # start_date = datetime.datetime.strptime(s[:8])
