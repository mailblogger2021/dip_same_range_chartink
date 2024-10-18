import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from fpdf import FPDF
import sys
import threading
import logging
import os
import json
import traceback
import datetime
import os
import telegram_message_send
import urllib.parse

def chartink_to_pdf(session,title, pdf,chartink_url):
    r = session.post('https://chartink.com/screener/process', data={'scan_clause': chartink_url}).json()
    df = pd.DataFrame(r['data'])
    if df.empty:
        return df
    
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(40, 20, title, ln=True)
    pdf.ln(2)
    
    table_cell_height = 6
    
    cols = df.columns
    content = df.values.tolist()
    
    max_widths = [pdf.get_string_width(col) for col in cols]
    for row in content:
        for i, cell in enumerate(row):
            width = pdf.get_string_width(str(cell))*50//100
            if width > max_widths[i]:
                max_widths[i] = width
    
    pdf.set_font('Arial', '', 6)
    cols = df.columns
    for i, col in enumerate(cols):
        pdf.cell(max_widths[i], table_cell_height, col, align='C', border=1)
    pdf.ln(table_cell_height)

    for row in content:
        for i, cell in enumerate(row):
            # Set cell width based on maximum content width in the column
            pdf.cell(max_widths[i], table_cell_height, str(cell), align='C', border=1)
        pdf.ln(table_cell_height)
    pdf.ln(10)

    return df
    # return df['nsecode'].unique().tolist()
    # else:
        # return []

def generate_chartink_code(time_frame_list=[], base_code_list=[], title_list=[], file_name='chartink_data_pdf'):
    pdf = FPDF(unit='mm', format=(250, 297))
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)

    ph_pl_list = {}
    df_list = {}
    # df_list_list = []
    with requests.Session() as session:
        r = session.get('https://chartink.com/screener/time-pass-48')
        soup = bs(r.content, 'lxml')
        session.headers['X-CSRF-TOKEN'] = soup.select_one('[name=csrf-token]')['content']
        for time_frame, base_code, title in zip(time_frame_list, base_code_list, title_list):
            df = chartink_to_pdf(session, title, pdf, base_code)
            ph_pl_list[time_frame] = df['nsecode'].unique().tolist() if not df.empty else []
            df_list[time_frame] = (df)
            # df_list_list.append(df)

    pdf.output(f'{file_name}.pdf', 'F')
    return ph_pl_list, df_list
    # return ph_pl_list,df_list_list

def append_to_excel(df_list,extra_details,excel_file='chartink_data.xlsx',alert_excel_file='today_alert_excel.xlsx'):
    current_time = datetime.datetime.now()
    if "find_stock_history_week" in extra_details:
        previous_week_date = current_time - datetime.timedelta(weeks=extra_details["find_stock_history_week"])
    current_time_str = current_time.strftime("%d/%m/%Y %H:%M:%S")

    # Get the ISO calendar details
    iso_calendar = current_time.isocalendar()

    # Extract the fiscal week number (ISO week number)
    fiscal_year_number = iso_calendar[0]
    fiscal_week_number = iso_calendar[1]
    fiscal_day_number = iso_calendar[2]

    previous_week_date = current_time - datetime.timedelta(weeks=1)
    # Get the ISO calendar details
    previous_iso_calendar = previous_week_date.isocalendar()

    # Extract the fiscal week number (ISO week number)
    previous_fiscal_year_number = previous_iso_calendar[0]
    previous_fiscal_week_number = previous_iso_calendar[1]
    previous_fiscal_day_number = previous_iso_calendar[2]

    # Find the date for Monday of the current week
    monday_date = current_time - datetime.timedelta(days=current_time.weekday())
    friday_date = monday_date + datetime.timedelta(days=4)

    fiscal_week = "FY"+str(fiscal_year_number)+"-FW"+str(fiscal_week_number)
    previous_fiscal_week = "FY"+str(previous_fiscal_year_number)+"-FW"+str(previous_fiscal_week_number)
    
    if os.path.exists(excel_file):
        sheets_dict = pd.read_excel(excel_file, engine="openpyxl", sheet_name=None)
    else:
        sheets_dict = {}
    
    if not df_list:
        return
    
    # for time_frame in df_list:
    #     if time_frame not in sheets_dict:
    #         sheets_dict[time_frame] = pd.DataFrame()
    #     df_list[time_frame]['timeframe'] = time_frame
    #     df_list[time_frame]['Date Time'] = current_time
    #     sheets_dict[time_frame] = pd.concat([sheets_dict[time_frame],df_list[time_frame]])

    merged_df = pd.DataFrame()
    for time_frame, df in df_list.items():  # Assuming df_list is a dictionary
        df['Date Time'] = current_time_str
        for extra_details_key in extra_details:
            if extra_details_key == time_frame:
                for key in extra_details[extra_details_key]:
                    df[key] = extra_details[extra_details_key][key]
        df["fiscal_week"] = fiscal_week
        df["previous_fiscal_week"] = previous_fiscal_week
        df["week_starting_date"] = monday_date.strftime("%d/%m/%Y")
        df["week_ending_date"] = friday_date.strftime("%d/%m/%Y")
        # df['timeframe'] = time_frame
    
        merged_df = pd.concat([df,merged_df], ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['nsecode']).reset_index(drop=True)
    # merged_df = pd.DataFrame()
    # for df in df_list:  # Assuming df_list is a dictionary
    #     # df['timeframe'] = time_frame
    #     df['Date Time'] = current_time_str
    #     merged_df = pd.concat([merged_df, df], ignore_index=True)

    if 'MergedData' not in sheets_dict:
        sheets_dict['MergedData'] = pd.DataFrame()

    sheets_dict['MergedData'] = pd.concat([sheets_dict['MergedData'], merged_df], ignore_index=True)

    with pd.ExcelWriter(excel_file) as writer:
        for sheet_name,df in sheets_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # stock_df = pd.read_excel(excel_file, sheet_name="MergedData")
    # df_fscl_wy = stock_df[(stock_df['fiscal_week'] == fiscal_week)]
    # df_fscl_wy1 = stock_df[(stock_df['fiscal_week'] == previous_fiscal_week)]

    # self_joined_df = pd.merge(df_fscl_wy, df_fscl_wy1,on='nsecode', how='left',suffixes=('_current', '_previous'))
    # self_joined_df = self_joined_df[self_joined_df["sr_previous"].isnull()].reset_index(drop=True).drop_duplicates(subset=['nsecode'])
    # self_joined_df = self_joined_df[["nsecode","name_current","Date Time_current","index_current","time_frame_current","fiscal_week_current","previous_fiscal_week_current","week_starting_date_current","week_ending_date_current"]]
    # self_joined_df.to_excel(alert_excel_file, index=False)
    return merged_df

def create_alert_excel_file(extra_details,excel_file='chartink_data.xlsx',alert_excel_file='today_alert_excel.xlsx'):
    current_time = datetime.datetime.now()
    if "find_stock_history_week" in extra_details:
        previous_week_date = current_time - datetime.timedelta(weeks=extra_details["find_stock_history_week"])
    current_time_str = current_time.strftime("%d/%m/%Y %H:%M:%S")

    # Get the ISO calendar details
    iso_calendar = current_time.isocalendar()

    # Extract the fiscal week number (ISO week number)
    fiscal_year_number = iso_calendar[0]
    fiscal_week_number = iso_calendar[1]
    fiscal_day_number = iso_calendar[2]

    # if(fiscal_day_number>=5):
        # return None,None # friday and weekend

    previous_week_date = current_time - datetime.timedelta(weeks=1)
    # Get the ISO calendar details
    previous_iso_calendar = previous_week_date.isocalendar()

    # Extract the fiscal week number (ISO week number)
    previous_fiscal_year_number = previous_iso_calendar[0]
    previous_fiscal_week_number = previous_iso_calendar[1]
    previous_fiscal_day_number = previous_iso_calendar[2]

    # Find the date for Monday of the current week
    monday_date = current_time - datetime.timedelta(days=current_time.weekday())
    friday_date = monday_date + datetime.timedelta(days=4)

    fiscal_week = "FY"+str(fiscal_year_number)+"-FW"+str(fiscal_week_number)
    previous_fiscal_week = "FY"+str(previous_fiscal_year_number)+"-FW"+str(previous_fiscal_week_number)
    
    stock_df = pd.read_excel(excel_file, sheet_name="MergedData")
    df_fscl_wy = stock_df[(stock_df['fiscal_week'] == fiscal_week)]
    df_fscl_wy1 = stock_df[(stock_df['fiscal_week'] == previous_fiscal_week)]

    self_joined_df = pd.merge(df_fscl_wy, df_fscl_wy1,on='nsecode', how='left',suffixes=('_current', '_previous'))
    # self_joined_df = self_joined_df[self_joined_df["sr_previous"].isnull()].reset_index(drop=True).drop_duplicates(subset=['nsecode'])
    self_joined_df = self_joined_df[self_joined_df["sr_previous"].isnull()].reset_index(drop=True)
    # self_joined_df = self_joined_df.groupby('nsecode', as_index=False).agg(lambda x: ', '.join(map(str, x.unique())))
    
    unique_nse_codes = self_joined_df['nsecode'].unique()
    # message = ',%0A'.join(unique_nse_codes)
    message = '\n'.join(unique_nse_codes)
    # message = urllib.parse.quote(message)
    telegram_message_send.send_message_with_documents(message)

    selected_columns = self_joined_df.filter(regex='_current$', axis=1).columns.tolist()
    columns_to_keep = ['nsecode'] + selected_columns
    self_joined_df_current = self_joined_df[columns_to_keep]
    self_joined_df_current.to_excel(alert_excel_file, index=False)
    return self_joined_df,self_joined_df_current

def create_fridays_alert_excel_file(extra_details,excel_file='chartink_data.xlsx',alert_excel_file='today_alert_excel.xlsx'):
    current_time = datetime.datetime.now()
    if "find_stock_history_week" in extra_details:
        previous_week_date = current_time - datetime.timedelta(weeks=extra_details["find_stock_history_week"])
    current_time_str = current_time.strftime("%d/%m/%Y %H:%M:%S")

    # Get the ISO calendar details
    iso_calendar = current_time.isocalendar()

    # Extract the fiscal week number (ISO week number)
    fiscal_year_number = iso_calendar[0]
    fiscal_week_number = iso_calendar[1]
    fiscal_day_number = iso_calendar[2]

    if(fiscal_day_number!=5): # friday
        return None,None # non friday and weekend

    previous_week_date = current_time - datetime.timedelta(weeks=1)
    # Get the ISO calendar details
    previous_iso_calendar = previous_week_date.isocalendar()

    # Extract the fiscal week number (ISO week number)
    previous_fiscal_year_number = previous_iso_calendar[0]
    previous_fiscal_week_number = previous_iso_calendar[1]
    previous_fiscal_day_number = previous_iso_calendar[2]

    # Find the date for Monday of the current week
    monday_date = current_time - datetime.timedelta(days=current_time.weekday())
    friday_date = monday_date + datetime.timedelta(days=4)

    fiscal_week = "FY"+str(fiscal_year_number)+"-FW"+str(fiscal_week_number)
    previous_fiscal_week = "FY"+str(previous_fiscal_year_number)+"-FW"+str(previous_fiscal_week_number)
    
    stock_df = pd.read_excel(excel_file, sheet_name="MergedData")
    df_fscl_wy = stock_df[(stock_df['fiscal_week'] == fiscal_week)]
    df_fscl_wy1 = stock_df[(stock_df['fiscal_week'] == previous_fiscal_week)]

    self_joined_df = pd.merge(df_fscl_wy, df_fscl_wy1,on='nsecode', how='left',suffixes=('_current', '_previous'))
    self_joined_df = self_joined_df[self_joined_df["sr_previous"].isnull()].reset_index(drop=True).drop_duplicates(subset=['nsecode'])
    
    selected_columns = self_joined_df.filter(regex='_current$', axis=1).columns.tolist()
    columns_to_keep = ['nsecode'] + selected_columns
    self_joined_df_current = self_joined_df[columns_to_keep]
    self_joined_df_current.to_excel(alert_excel_file, index=False)
    return self_joined_df,self_joined_df_current

if __name__ =="__main__":
    index_details = {
        "NIFTY 50" : "33492",
        # "NIFTY 100": "33619",
        "NIFTY NEXT 50" : "1116352",
        "NIFTY 200": "46553",
        "MIDCAP 50" : "136492",
        "MIDCAP 100" : "1090585",
        "MIDCAP 150" : "1090588",
        "MIDCAP SELECT" : "1090579",
        # "NIFTY 500" : "57960",
        # "NIFTY 500 multicap50:25:25" : "1090574",
        # "NIFTY 500 multicap50:25:25" : "1090574",
        "SMALLCAP 50" : "1090568",
        "SMALLCAP 100" : "1090587",
        # "SMALLCAP 250" : "1090572",
    }

    week_count_list = {
        # "52":0.50,
        # "35":0.60,
        "26":0.70,
        # "8":0.80
    }

    same_level_chartink_code = {
        "{{{index_name}}}":"( {{{index}}} ( ( {{{index}}} ( 25 weeks ago max ( 25 , weekly high ) <= weekly max ( 50 , weekly high ) * 1.02 and 25 weeks ago max ( 25 , weekly low ) >= weekly max ( 50 , weekly low ) * 0.98 and 25 weeks ago min ( 25 , weekly low ) <= weekly close * 1.01 and 25 weeks ago max ( 25 , weekly low ) >= weekly close * 0.99 and ( {{{index}}} ( 15 weeks ago max ( 10 , weekly high ) <= weekly max ( 25 , weekly high ) * 1.02 and 15 weeks ago max ( 10 , weekly high ) >= weekly max ( 25 , weekly high ) * 0.98 and 15 weeks ago min ( 10 , weekly low ) <= weekly min ( 25 , weekly low ) * 1.02 and 15 weeks ago max ( 10 , weekly low ) >= weekly max ( 25 , weekly low ) * 0.98 ) ) ) ) ) ) "
    }

    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%Y_%m_%d.%H_%M_%S")
    os.makedirs(f"report/same_level/pdf/", exist_ok=True)
    os.makedirs(f"report/same_level/excel/", exist_ok=True)
    os.makedirs(f"report/same_level/alert/", exist_ok=True)

    pdf_name = f"report/same_level/pdf/same_level_chartink_{date_time}"
    excel_file_name = f"report/same_level/excel/same_level.xlsx"
    alert_excel_file = f"report/same_level/alert/same_level_alert_{date_time}.xlsx"

    base_code_list, title_list, time_frame_list = [], [], []
    extra_details = {}

    for index_name in index_details:
        index = index_details[index_name]
        for chartink_title in same_level_chartink_code:
            base_code = same_level_chartink_code[chartink_title].format(index=index)
            time_frame_list.append(f"{index_name}")
            extra_details[time_frame_list[-1]] = {
                "index":index_name,
                "time_frame":"week",
                "before_week":0,
                "find_stock_history_week" : 0
                }
            base_code_list.append(base_code)
            title_list.append(chartink_title.format(index_name=index_name))


    ph_pl_list, df_list = generate_chartink_code(time_frame_list,base_code_list,title_list,pdf_name)
    merged_df = append_to_excel(df_list,extra_details, excel_file=excel_file_name,alert_excel_file=alert_excel_file)
    self_joined_df,self_joined_df_current = create_alert_excel_file(extra_details, excel_file=excel_file_name,alert_excel_file=alert_excel_file)

    index_details = {
        "NIFTY 50" : "33492",
        # "NIFTY 100": "33619",
        "NIFTY NEXT 50" : "1116352",
        "NIFTY 200": "46553",
        "MIDCAP 50" : "136492",
        "MIDCAP 100" : "1090585",
        "MIDCAP 150" : "1090588",
        "MIDCAP SELECT" : "1090579",
        # "NIFTY 500" : "57960",
        "NIFTY 500 multicap50:25:25" : "1090574",
        # "NIFTY 500 multicap50:25:25" : "1090574",
        "SMALLCAP 50" : "1090568",
        "SMALLCAP 100" : "1090587",
        # "SMALLCAP 250" : "1090572",
    }

    week_count_list = {
        "52":0.50,
        "35":0.60,
        "26":0.70,
        "8":0.80
    }

    drop_some_percentage_from_with_in_year = { # dip
        "{index_name}_{week_count}_{percentage}":"( {{{index}}} ( weekly close <= weekly max ( {week_count} , weekly high ) * {percentage} ) ) "
        }

    current_time = datetime.datetime.now()
    date_time = current_time.strftime("%Y_%m_%d.%H_%M_%S")
    os.makedirs(f"report/dip/pdf/", exist_ok=True)
    os.makedirs(f"report/dip/excel/", exist_ok=True)
    os.makedirs(f"report/dip/alert/", exist_ok=True)

    pdf_name = f"report/dip/pdf/dip_chartink_{date_time}"
    excel_file_name = f"report/dip/excel/dip.xlsx"
    alert_excel_file = f"report/dip/alert/dip_alert_{date_time}.xlsx"

    base_code_list, title_list, time_frame_list = [], [], []
    extra_details = {}
    for index_name in index_details:
        index = index_details[index_name]
        for week_count, percentage in week_count_list.items():
            for chartink_title in drop_some_percentage_from_with_in_year:
                base_code = drop_some_percentage_from_with_in_year[chartink_title].format(index=index, week_count=int(week_count), percentage=percentage)
                time_frame_list.append(f"{index_name}_{week_count}_{percentage}_per")
                extra_details[time_frame_list[-1]] = {
                    "index":index_name,
                    "time_frame":f"{week_count}week",
                    "percentage":round(1-percentage,2)*100,
                    "before_week":0,
                    "find_stock_history_week" : 0
                    }
                base_code_list.append(base_code)
                title_list.append(chartink_title.format(index_name=index_name, week_count=int(week_count), percentage=round(1-percentage,2)*100))

    ph_pl_list, df_list = generate_chartink_code(time_frame_list,base_code_list,title_list,pdf_name)
    merged_df = append_to_excel(df_list,extra_details, excel_file=excel_file_name,alert_excel_file=alert_excel_file)
    self_joined_df,self_joined_df_current = create_alert_excel_file(extra_details, excel_file=excel_file_name,alert_excel_file=alert_excel_file)


    # index_details = {
    #     "NIFTY 50" : "33492",
    #     # "NIFTY 100": "33619",
    #     "NIFTY NEXT 50" : "1116352",
    #     # "NIFTY 200": "46553",
    #     "MIDCAP 50" : "136492",
    #     # "MIDCAP 100" : "1090585",
    #     # "MIDCAP 150" : "1090588",
    #     "MIDCAP SELECT" : "1090579",
    #     # "NIFTY 500" : "57960",
    #     # "NIFTY 500 multicap50:25:25" : "1090574",
    #     # "NIFTY 500 multicap50:25:25" : "1090574",
    #     "SMALLCAP 50" : "1090568",
    #     # "SMALLCAP 100" : "1090587",
    #     # "SMALLCAP 250" : "1090572",
    # }

    # week_count_list = {
    #     # "52":0.50,
    #     # "35":0.60,
    #     "26":0.70,
    #     # "8":0.80
    # }
    # max_week_count = 160 # 4 years

    # year_count_list = {}
    # max_year_count = 10
    # history_dip_weekcode = {
    #         "{index_name}_{week_count}_{percentage}_{this_week_count}_week":"( {{{index}}} ( {this_week_count} week ago close <= {this_week_count} week ago max ( {week_count} , weekly high ) * {percentage} ) ) ",
    #     }

    # current_time = datetime.datetime.now()
    # date_time = current_time.strftime("%Y_%m_%d.%H_%M_%S")
    # os.makedirs(f"report/history_dip/pdf/", exist_ok=True)
    # os.makedirs(f"report/history_dip/excel/", exist_ok=True)
    # os.makedirs(f"report/history_dip/alert/", exist_ok=True)

    # pdf_name = f"report/history_dip/pdf/history_dip_chartink_{date_time}"
    # excel_file_name = f"report/history_dip/excel/history_dip.xlsx"
    # alert_excel_file = f"report/history_dip/alert/history_dip_alert_{date_time}.xlsx"


    # base_code_list, title_list, time_frame_list = [], [], []
    # extra_details = {}

    # for index_name in index_details:
    #     index = index_details[index_name]
    #     for this_week_count in range(1,max_week_count):
    #         for week_count, percentage in week_count_list.items():
    #             for chartink_title in history_dip_weekcode:
    #                 base_code = history_dip_weekcode[chartink_title].format(index=index, week_count=int(week_count), percentage=percentage,this_week_count=this_week_count)
    #                 time_frame_list.append(f"{index_name} {week_count} week {percentage} per {this_week_count} week before")
    #                 extra_details[time_frame_list[-1]] = {
    #                     "index":index_name,
    #                     "time_frame":"week",
    #                     "before_week":0,
    #                     "find_stock_history_week" : this_week_count
    #                     }
    #                 base_code_list.append(base_code)
    #                 title_list.append(chartink_title.format(index_name=index_name, week_count=int(week_count), percentage=round(1-percentage,2)*100,this_week_count=this_week_count))

    #     for this_year_count in range(1,max_year_count):
    #         pass

    # ph_pl_list, df_list = generate_chartink_code(time_frame_list,base_code_list,title_list,pdf_name)
    # merged_df = append_to_excel(df_list,extra_details, excel_file=excel_file_name,alert_excel_file=alert_excel_file)
    # self_joined_df,self_joined_df_current = create_alert_excel_file(extra_details, excel_file=excel_file_name,alert_excel_file=alert_excel_file)
