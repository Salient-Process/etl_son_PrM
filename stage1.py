import os
import re
import zipfile
import logging
import pandas as pd


def extractZip(config, file_path, work_dir, output_dir):
    """
        a.	Unpack the zip file
        b.	Transform the input CSV files it contains into output CSV files, none of which should be > 300MB 
        c.	Places the output CSV files into output_dir
    """
    logging.info(f"Enter in Stage1 {file_path}")
    zip_file_name = os.path.basename(file_path)
    logging.info(f"Zip Name {zip_file_name}")

    # Throw artificial error to debug error processing
    if re.search("error", zip_file_name, re.IGNORECASE):
        raise Exception("Simulated exception because the zip file name contains the word 'error'") 

    # Extract file and place in output_dir
    with zipfile.ZipFile(file_path, 'r') as f:
        f.extractall(work_dir)
    os.remove(file_path)
    """"
    # Move any CSV file to stage2_csv_dir
    for path in os.listdir(work_dir):
        full_path = os.path.join(work_dir, path)

        if re.search(r"\.csv\Z", path, re.IGNORECASE):
            shutil.move(full_path, output_dir)
    """
    return

def format_datetime(dt_series):

    def get_split_date(strdt):
        split_date = strdt.split('-')
        str_date = split_date[0] + '-' +split_date[1]+ '-' + split_date[2][:3] + ' ' + split_date[2][3:]
        return str_date

    dt_series = pd.to_datetime(dt_series.apply(lambda x: get_split_date(x)), format = '%d-%b-%y %H:%M:%S')

    return dt_series

def formatData(path,finalPath):

    df = pd.read_csv(os.path.join(path,'PO to Pay Process Data (2).csv'),encoding= 'unicode_escape',low_memory=False)
    dfa = pd.read_csv(os.path.join(path,'PO Line Processing Errors (1).csv'),encoding= 'unicode_escape',low_memory=False)
    dfb = pd.read_csv(os.path.join(path,'Interface PO Line Receipt Processing Error (1).csv'),encoding= 'unicode_escape',low_memory=False)
    dfC = pd.read_csv(os.path.join(path,'PO Line Receipt is Created (1).csv'),encoding= 'unicode_escape',low_memory=False)


    #df['Start Timestamp'] = format_datetime(df['Start Timestamp'])
    #Change the blank spaces in source system for ORC-ORACLE in a new
    df1=df['SOURCE_SYSTEM'].fillna('ORC', inplace=True)

    #Select the source system that are going to be analyze
    condition_column1 = 'SOURCE_SYSTEM'
    condition_value1 = ['ARI','DE4','IT1','FI1']
    df1 = df[~df[condition_column1].isin(condition_value1)]

    df2 = df1.copy()
    
    df2['Start Timestamp'] = df2['Start Timestamp'].shift(1)
    df2.at[0, 'Start Timestamp'] = '01-Jan-00 00:00:00'

    df2.drop_duplicates(inplace=True)

    df2['Hold Type'] = df2['HOLD_LOOKUP_CODE']


    #Require to Delete Dist Variance
    condition_column2 = 'HOLD_LOOKUP_CODE'
    condition_value2 = ['DIST VARIANCE']
    df2 = df2[~df2[condition_column1].isin(condition_value1)]
    
    df2['Hold Type'] = df2['Hold Type'].replace(['AMOUNT', 'AWT ERROR', 'DIST VARIANCE','Duplicate Invoice', 'INSUFFICIENT LINE INFO', 'DIST VARIANCE','LINE VARIANCE','NO RATE','PRICE','QTY REC','TAX VARIANCE'], 'system')

    df2['Hold Type'] = df2['Hold Type'].replace(['BE Approval Hold', 'Credit Needs GL/Approval', 'Incomplete Invoice','Inv. UOM is Different', 'INVALID PO','Invalid PO Numbr(Non-Orac','Invoice UOM Does Not Matc','Invoice UOM is different','Missing Consumption Repor','Multiple Matching Receipt','No matching item Desc.','No Matching Receipt','No Receipt','No Receipt Available(Non-','No Receipts Available','No Valid PO','Non-PO Charge/Fee','Pay When Paid','PO NOT APPROVED','PO or Payment Request Req','PO Vend/Paymt Site diff.','PO/Invoice Supplier Misma','Remit Address Mismatch'], 'manual')

    df2.loc[df2['Hold Type'] == 'system', 'ACTIVITY'] = 'Invoice Line Hold System'
    df2.loc[df2['Hold Type'] == 'manual', 'ACTIVITY'] = 'Invoice Line Hold Manual'

    dfa.rename(columns={'Activity': 'ACTIVITY'}, inplace=True)
    combined_df = pd.concat([dfa,dfb], ignore_index=True)
    df_final = pd.concat([df2,combined_df], ignore_index=True)

    new_df = df_final[df_final['ACTIVITY'] != 'PO Line Receipt is Created']

    Final_df_v2 = pd.concat([new_df,dfC], ignore_index=True)

    Final_df_v2['Start Timestamp'] = format_datetime(Final_df_v2['Start Timestamp'])

    condition_column1 = 'HOLD_LOOKUP_CODE'
    condition_value1 = ['DIST VARIANCE']
    final = Final_df_v2[~Final_df_v2[condition_column1].isin(condition_value1)]

    write_in_chunks(final,'SonocoFILE2.csv','SonocoFILE2',finalPath)

def write_in_chunks(df, file_name,file0,path):
    lines_per_file = 80000
    file = os.path.join(path,file_name)
    df.to_csv(file,index = False)

    with open(file, 'r', encoding='utf-8') as file:
        header = file.readline()  # read the header
        file_num = 1        
        while True:
            lines = [file.readline() for _ in range(lines_per_file)]
            if not lines[0]:  # stop when end of file is reached
                break
            fileName = os.path.join(path,f'{file0}_{file_num}.csv')
            with open(fileName, 'w', encoding='utf-8') as out_file:
                out_file.write(header)  # write the header to each output file
                out_file.writelines(lines)
            file_num += 1
    os.remove(path+'/'+file_name)

