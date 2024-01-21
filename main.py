def scanner(request):
    import numpy as np
    import pandas as pd
    from google.cloud import storage
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    data = pd.DataFrame(columns=['SYMBOL', 'NR4', 'NR7', 'BUL_REV', 'CONSOLIDATION', 'PRICE', 'VOLUME', 'DEL',
                                 'DEL_PER', 'QT', 'COI', 'PCR_T', 'PCR_VAL', '10M_CE', '10M_CE_LTD', '10M_PE',
                                 '10M_PE_LTD',
                                 'P_POS'])
    FNO_WL = pd.read_csv("gs://bba_support_files/WL_FNO.csv")
    row_index = 0
    data['SYMBOL'] = FNO_WL['Symbol'].values
    # data['SYMBOL'] = ["TATAMOTORS", "AARTIIND", "ITC"]

    for item in data["SYMBOL"]:
        try:
            print(item)
            dropdown_value = item
            dropdown_n_days_value = 30
            short_sma = 2
            medium_sma = 7
            long_sma = 21
            b_band = 1.5
            kc = 1.2

            client = bigquery.Client()
            sql_stock = f"""
                SELECT TIMESTAMP, CUR_FUT_EXPIRY_DT,NEAR_FUT_EXPIRY_DT,
                                    SYMBOL, EQ_OPEN_PRICE, EQ_HIGH_PRICE, EQ_LOW_PRICE, EQ_CLOSE_PRICE,
                                    EQ_TTL_TRD_QNTY, EQ_DELIV_QTY, EQ_DELIV_PER, EQ_QT,
                                    CUR_PE_STRIKE_PR_OIMAX, CUR_PE_STRIKE_PR_10MVOL,
                                    CUR_CE_STRIKE_PR_OIMAX, CUR_CE_STRIKE_PR_10MVOL,
                                    NEAR_CE_STRIKE_PR_OIMAX, NEAR_CE_STRIKE_PR_10MVOL,
                                    NEAR_PE_STRIKE_PR_OIMAX, NEAR_PE_STRIKE_PR_10MVOL,
                                    CUR_PE_OI_SUM, CUR_CE_OI_SUM,
                                    EQ_CHG_PER, FUT_COI, FUT_BUILD_UP,FUT_PRICE_COL, FUT_COI_EXPLOSION_COL,
                                    CUR_PCR, NEAR_PCR, BAR, QTCO0321, QTCO0321COL
                FROM `phrasal-fire-373510.Big_Bull_Analysis.Master_Data`
                WHERE SYMBOL = '{dropdown_value}'
                ORDER BY TIMESTAMP DESC LIMIT {dropdown_n_days_value}
            """
            df_stock = client.query(sql_stock).to_dataframe()
            df_stock = df_stock.sort_values(by='TIMESTAMP', ascending=True)
            df_stock["BAR"] = df_stock["BAR"].astype(int)
            df_stock = df_stock.reset_index()
            df_stock["CUR_CE_STRIKE_PR_10MVOL"] = df_stock["CUR_CE_STRIKE_PR_10MVOL"].astype(str)
            df_stock["CUR_PE_STRIKE_PR_10MVOL"] = df_stock["CUR_PE_STRIKE_PR_10MVOL"].astype(str)
            df_stock.to_csv("Sample_Data.csv")
            # print(df_stock)

            # NR4 INDICATOR___________________________________________________________
            df_stock["ROLL_MAX_4"] = df_stock["EQ_HIGH_PRICE"].rolling(4).max()
            df_stock["ROLL_MIN_4"] = df_stock["EQ_LOW_PRICE"].rolling(4).min()
            df_stock["NR4"] = np.where((df_stock["EQ_HIGH_PRICE"].iloc[-4] == df_stock["ROLL_MAX_4"].iloc[-1]) &
                                       (df_stock["EQ_LOW_PRICE"].iloc[-4] == df_stock["ROLL_MIN_4"].iloc[-1]), 'Y', '')

            # NR7 INDICATOR___________________________________________________________
            df_stock["ROLL_MAX_7"] = df_stock["EQ_HIGH_PRICE"].rolling(7).max()
            df_stock["ROLL_MIN_7"] = df_stock["EQ_LOW_PRICE"].rolling(7).min()
            df_stock["NR7"] = np.where((df_stock["EQ_HIGH_PRICE"].iloc[-7] == df_stock["ROLL_MAX_7"].iloc[-1]) &
                                       (df_stock["EQ_LOW_PRICE"].iloc[-7] == df_stock["ROLL_MIN_7"].iloc[-1]), 'Y', '')

            # BULLISH REVERSAL INDICATOR___________________________________________________________
            df_stock["BR_CNDL_1"] = np.where((df_stock["EQ_CLOSE_PRICE"].iloc[-3] < df_stock["EQ_OPEN_PRICE"].iloc[-3]),
                                             'YES', 'NO')
            df_stock["BR_CNDL_2"] = np.where((df_stock["EQ_OPEN_PRICE"].iloc[-2] > df_stock["EQ_LOW_PRICE"].iloc[-2]) & \
                                             (df_stock["EQ_CLOSE_PRICE"].iloc[-2] > df_stock["EQ_OPEN_PRICE"].iloc[
                                                 -2]) & \
                                             (df_stock["EQ_HIGH_PRICE"].iloc[-2] >= df_stock["EQ_CLOSE_PRICE"].iloc[
                                                 -2]) & \
                                             ((df_stock["EQ_HIGH_PRICE"].iloc[-2] - df_stock["EQ_CLOSE_PRICE"].iloc[
                                                 -2]) < ((df_stock["EQ_CLOSE_PRICE"].iloc[-2] -
                                                          df_stock["EQ_OPEN_PRICE"].iloc[-2]))) & \
                                             ((df_stock["EQ_OPEN_PRICE"].iloc[-2] - df_stock["EQ_LOW_PRICE"].iloc[
                                                 -2]) >= (3 * (df_stock["EQ_CLOSE_PRICE"].iloc[-2] -
                                                               df_stock["EQ_OPEN_PRICE"].iloc[-2]))), 'YES', 'NO')
            df_stock["BR_CNDL_4"] = np.where(
                (df_stock["EQ_CLOSE_PRICE"].iloc[-1] > df_stock["EQ_OPEN_PRICE"].iloc[-1]) &
                (df_stock["EQ_CLOSE_PRICE"].iloc[-1] > df_stock["EQ_HIGH_PRICE"].iloc[-3]), 'YES',
                'NO')
            df_stock["BUL_REV"] = np.where(((df_stock["BR_CNDL_1"] == 'YES') & (df_stock["BR_CNDL_2"] == 'YES') & (
                        df_stock["BR_CNDL_4"] == 'YES')), 'Y', '')

            # BEARISH REVERSAL INDICATOR___________________________________________________________

            # CONSOLIDATION PHASE INDICATOR________________________________________________________
            df_stock['SMA'] = df_stock['EQ_CLOSE_PRICE'].rolling(
                window=long_sma).mean()  # Simple Moving Average calculation (period = 20)
            df_stock['stdev'] = df_stock['EQ_CLOSE_PRICE'].rolling(
                window=long_sma).std()  # Standard Deviation calculation
            df_stock['Lower_Bollinger'] = df_stock['SMA'] - (
                    b_band * df_stock['stdev'])  # Calculation of the lower curve of the Bollinger Bands
            df_stock['Upper_Bollinger'] = df_stock['SMA'] + (b_band * df_stock['stdev'])  # Upper curve

            df_stock['TR'] = abs(df_stock['EQ_HIGH_PRICE'] - df_stock['EQ_LOW_PRICE'])  # True Range calculation
            df_stock['ATR'] = df_stock['TR'].rolling(window=long_sma).mean()  # Average True Range

            df_stock['Upper_KC'] = df_stock['SMA'] + (kc * df_stock['ATR'])  # Upper curve of the Keltner Channel
            df_stock['Lower_KC'] = df_stock['SMA'] - (kc * df_stock['ATR'])  # Lower curve

            df_stock['consolidation'] = np.where(
                (df_stock['Lower_Bollinger'] > df_stock['Lower_KC']) & (
                            df_stock['Upper_Bollinger'] < df_stock['Upper_KC']),
                "Y", "")

            # PRICE STRENGTH INDICATOR___________________________________________________________
            df_stock["CLOSE_MA_S"] = df_stock["EQ_CLOSE_PRICE"].rolling(short_sma).mean()
            df_stock["CLOSE_MA_M"] = df_stock["EQ_CLOSE_PRICE"].rolling(medium_sma).mean()
            df_stock["CLOSE_MA_COL"] = np.where((df_stock["CLOSE_MA_S"] > df_stock["CLOSE_MA_M"].shift(2)), 'S', '')

            # VOLUME STRENGTH INDICATOR___________________________________________________________
            df_stock["VOLUME_MA_S"] = df_stock["EQ_TTL_TRD_QNTY"].rolling(short_sma).mean()
            df_stock["VOLUME_MA_M"] = df_stock["EQ_TTL_TRD_QNTY"].rolling(medium_sma).mean()
            df_stock["VOLUME_MA_COL"] = np.where((df_stock["VOLUME_MA_S"] > df_stock["VOLUME_MA_M"].shift(2)), 'S', '')

            # DELIVERY QUANTITY STRENGTH INDICATOR___________________________________________________________
            df_stock["DEL_MA_S"] = df_stock["EQ_DELIV_QTY"].rolling(short_sma).mean()
            df_stock["DEL_MA_M"] = df_stock["EQ_DELIV_QTY"].rolling(medium_sma).mean()
            df_stock["DEL_MA_COL"] = np.where((df_stock["DEL_MA_S"] > df_stock["DEL_MA_M"].shift(2)), 'S', '')

            # DELIVERY PERCENTAGE STRENGTH INDICATOR___________________________________________________________
            df_stock["DEL_PER_MA_S"] = ((df_stock["EQ_TTL_TRD_QNTY"].rolling(short_sma).sum() /
                                         df_stock["EQ_DELIV_QTY"].rolling(short_sma).sum()) * 100).round(1)
            df_stock["DEL_PER_MA_M"] = ((df_stock["EQ_TTL_TRD_QNTY"].rolling(medium_sma).sum() /
                                         df_stock["EQ_DELIV_QTY"].rolling(medium_sma).sum()) * 100).round(1)
            df_stock["DEL_PER_MA_COL"] = np.where((df_stock["DEL_PER_MA_S"] < df_stock["DEL_PER_MA_M"].shift(2)), 'W',
                                                  '')

            # Q/T STRENGTH INDICATOR___________________________________________________________
            df_stock["QT_MA_S"] = df_stock["EQ_QT"].rolling(short_sma).mean()
            df_stock["QT_MA_M"] = df_stock["EQ_QT"].rolling(medium_sma).mean()
            df_stock["QT_MA_COL"] = np.where((df_stock["QT_MA_S"] > df_stock["QT_MA_M"].shift(2)), 'S', '')

            # COI STRENGTH INDICATOR___________________________________________________________
            df_stock["COI_MA_S"] = df_stock["FUT_COI"].rolling(short_sma).mean()
            df_stock["COI_MA_M"] = df_stock["FUT_COI"].rolling(medium_sma).mean()
            df_stock["COI_MA_COL"] = np.where((df_stock["COI_MA_S"] > df_stock["COI_MA_M"].shift(2)), 'S', '')

            # PCR STRENGTH INDICATOR___________________________________________________________
            df_stock["CUR_PCR_MA_S"] = df_stock["CUR_PCR"].rolling(short_sma).mean()
            df_stock["CUR_PCR_MA_M"] = df_stock["CUR_PCR"].rolling(medium_sma).mean()
            df_stock["CUR_PCR_MA_COL"] = np.where((df_stock["CUR_PCR_MA_S"] > df_stock["CUR_PCR_MA_M"].shift(2)), 'S',
                                                  '')

            # PCR VALUE STRENGTH INDICATOR___________________________________________________________
            df_stock["PCR_MA_COL"] = np.where(df_stock["CUR_PCR"] >= 100, 'S',
                                              np.where((df_stock["CUR_PCR"] < 100) & (df_stock["CUR_PCR"] > 80), 'N',
                                                       'W'))

            # 10 MILLION OPTION VOLUME (CE) ___________________________________________________________D)
            found_10M_CE = df_stock[df_stock['CUR_CE_STRIKE_PR_10MVOL'].str.contains('nan')]
            if found_10M_CE["CUR_CE_STRIKE_PR_10MVOL"].count() == len(df_stock["CUR_CE_STRIKE_PR_10MVOL"]):
                CE_value = ""
            else:
                CE_value = "Y"
            # 10 MILLION OPTION VOLUME LTD (CE) ___________________________________________________________
            df_stock["10M_CE_LTD"] = np.where(df_stock["CUR_CE_STRIKE_PR_10MVOL"] == "nan", "", 'Y')

            # 10 MILLION OPTION VOLUME (PE) ___________________________________________________________D)
            found_10M_PE = df_stock[df_stock['CUR_PE_STRIKE_PR_10MVOL'].str.contains('nan')]
            if found_10M_PE["CUR_PE_STRIKE_PR_10MVOL"].count() == len(df_stock["CUR_PE_STRIKE_PR_10MVOL"]):
                PE_value = ""
            else:
                PE_value = "Y"

            # 10 MILLION OPTION VOLUME (PE) ___________________________________________________________
            df_stock["10M_PE_LTD"] = np.where(df_stock["CUR_PE_STRIKE_PR_10MVOL"] == "nan", "", 'Y')
            # NUMBER OF STRIKE PRICE WITH 10 MILLION OPTION VOLUME (CE) _______________________________
            # NUMBER OF STRIKE PRICE WITH 10 MILLION OPTION VOLUME (PE) _______________________________

            # DISTANCE(IN PERCENT) FROM RESISTANCE(MAX CE)___________________________________________________________
            df_stock["R_DIST"] = ((df_stock["CUR_CE_STRIKE_PR_OIMAX"] - df_stock["EQ_CLOSE_PRICE"]) / df_stock[
                "EQ_CLOSE_PRICE"] * 100).round(1)
            # DISTANCE(IN PERCENT) FROM SUPPORT (MAX CE)___________________________________________________________
            df_stock["S_DIST"] = ((df_stock["EQ_CLOSE_PRICE"] - df_stock["CUR_PE_STRIKE_PR_OIMAX"]) / df_stock[
                "EQ_CLOSE_PRICE"] * 100).round(1)
            df_stock["RANGE"] = df_stock["CUR_CE_STRIKE_PR_OIMAX"] - df_stock["CUR_PE_STRIKE_PR_OIMAX"]
            df_stock["RANGE_P90"] = df_stock["CUR_CE_STRIKE_PR_OIMAX"] - (.1 * df_stock["RANGE"])
            df_stock["RANGE_P10"] = df_stock["CUR_PE_STRIKE_PR_OIMAX"] + (.1 * df_stock["RANGE"])
            df_stock["P_POS"] = np.where(((df_stock["EQ_CLOSE_PRICE"] > df_stock["CUR_PE_STRIKE_PR_OIMAX"]) &
                                          (df_stock["EQ_CLOSE_PRICE"] < df_stock["RANGE_P10"])), "N SUP",
                                         np.where(((df_stock["EQ_CLOSE_PRICE"] < df_stock["CUR_CE_STRIKE_PR_OIMAX"]) &
                                                   (df_stock["EQ_CLOSE_PRICE"] > df_stock["RANGE_P90"])), "N RES", ""))

            # Insert Scanned Values in Dataframe
            data.loc[data.index[row_index], 'SYMBOL'] = df_stock["SYMBOL"].iloc[-1]
            data.loc[data.index[row_index], 'NR4'] = df_stock["NR4"].iloc[-1]
            data.loc[data.index[row_index], 'NR7'] = df_stock["NR7"].iloc[-1]
            data.loc[data.index[row_index], 'BUL_REV'] = df_stock["BUL_REV"].iloc[-1]
            data.loc[data.index[row_index], 'CONSOLIDATION'] = df_stock["consolidation"].iloc[-1]
            data.loc[data.index[row_index], 'PRICE'] = df_stock["CLOSE_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'VOLUME'] = df_stock["VOLUME_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'DEL'] = df_stock["DEL_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'DEL_PER'] = df_stock["DEL_PER_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'QT'] = df_stock["QT_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'COI'] = df_stock["COI_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'PCR_T'] = df_stock["CUR_PCR_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], 'PCR_VAL'] = df_stock["PCR_MA_COL"].iloc[-1]
            data.loc[data.index[row_index], '10M_CE'] = CE_value
            data.loc[data.index[row_index], '10M_CE_LTD'] = df_stock["10M_CE_LTD"].iloc[-1]
            data.loc[data.index[row_index], '10M_PE'] = PE_value
            data.loc[data.index[row_index], '10M_PE_LTD'] = df_stock["10M_PE_LTD"].iloc[-1]
            data.loc[data.index[row_index], 'P_POS'] = df_stock["P_POS"].iloc[-1]
            row_index = row_index + 1
            # print(data)
            df_stock.to_csv("scanner_test_data.csv")
        except Exception:
            pass

    # print(data)
    # data.to_csv("scanner.csv")
    # try:
    #     client_storage = storage.Client()
    #     bucket = client_storage.bucket('biswasp87')
    #     blob = bucket.blob('Scanner.csv')
    #     blob.upload_from_string(data.to_csv(), 'text/csv')
    # except Exception:
    #     pass
    # _________________________________________________________________________________
    # UPLOAD DATA TO BIG QUERY
    # _________________________________________________________________________________
    data.astype(str)
    client = bigquery.Client()
    try:
        table_id = "phrasal-fire-373510.Scanner_Data.FNO_Scanner"
        # project = "WRITE_APPEND"
        project = "WRITE_TRUNCATE"
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[

                bigquery.SchemaField("SYMBOL", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("NR4", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("NR7", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("BUL_REV", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("CONSOLIDATION", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("PRICE", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("VOLUME", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("DEL", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("DEL_PER", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("QT", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("COI", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("PCR_T", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("PCR_VAL", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("10M_CE", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("10M_CE_LTD", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("10M_PE", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("10M_PE_LTD", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("P_POS", "STRING", mode="NULLABLE"),

            ],
            write_disposition=project,)
        try:
            client.get_table(table_id)  # Make an API request.
            print("Table {} already exists.".format(table_id))
            # Upload Current Dataframe
            job = client.load_table_from_dataframe(data, table_id,
                                                   job_config=job_config)  # Make an API request.
            job.result()  # Wait for the job to complete.

            table = client.get_table(table_id)  # Make an API request.
            print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
            print("Last Updated on: {}".format(table.modified))

        except NotFound:
            print("Table {} is not found. Proceed to create table".format(table_id))
            client.create_table(table_id)  # API request
            print(f"Created {table_id}.")
            # Upload Current Dataframe
            job = client.load_table_from_dataframe(data, table_id,
                                                   job_config=job_config)  # Make an API request.
            job.result()  # Wait for the job to complete.

            table = client.get_table(table_id)  # Make an API request.
            print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
            print("Last Updated on: {}".format(table.modified))
    except Exception as e:
        print(f"***** ERROR at fetching : {e} ****")  # Print the ERROR Message
    return ("Done!", 200)
