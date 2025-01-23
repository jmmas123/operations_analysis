import os
from utils import get_base_output_path
from rich.progress import Progress
import time
import pandas as pd
import random
import numpy as np
from datetime import datetime
import socket


def billing_data_reconstruction(saldo_inv_cliente_fact, resumen_mensual_ingresos_fact, resumen_despachos_cliente_fact,
                                start_date, end_date, registro_ingresos, supplier_info):

    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Processing and analyzing Client's operational Data: ", total=44)

        resumen_mensual_ingresos_fact['fecha_x'] = pd.to_datetime(resumen_mensual_ingresos_fact['fecha_x'])
        resumen_despachos_cliente_fact['fecha_x'] = pd.to_datetime(resumen_despachos_cliente_fact['fecha_x'])
        resumen_mensual_ingresos_fact['ddma'] = resumen_mensual_ingresos_fact['ddma'].fillna("")

        supplier_info = supplier_info.loc[:,['idcontacto', 'descrip']]

        supplier_info['idcontacto'] = supplier_info['idcontacto'].fillna("")
        supplier_info['descrip'] = supplier_info['descrip'].fillna("")

        # Step 3: Rename columns as needed
        supplier_info = supplier_info.rename(columns={
            'descrip': 'Client'
        })

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # # Save the final DataFrame to CSV
        # output_path = os.path.join(get_base_output_path(), 'resumen_mensual_ingresos_fact.csv')
        # resumen_mensual_ingresos_fact.to_csv(output_path, index=False)
        # output_path = os.path.join(get_base_output_path(), 'saldo_inv_cliente_fact.csv')
        # saldo_inv_cliente_fact.to_csv(output_path, index=False)

        # *** INFLOW CBM AND PALLETS ***

        # Replace values in 'idubica1' that start with 'R' with an empty string
        saldo_inv_cliente_fact['idubica1'] = saldo_inv_cliente_fact['idubica1'].str.replace(r'^R.*', '', regex=True)
        saldo_inv_cliente_fact['idubica1'] = saldo_inv_cliente_fact['idubica1'].str.replace(r'^TM.*', '', regex=True)

        # Replace values in 'idubica1' that do not start with 'TA' with an empty string
        mask = ~saldo_inv_cliente_fact['idubica1'].str.startswith('TA', na=False)
        saldo_inv_cliente_fact.loc[mask, 'idubica1'] = ''

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Fill empty values in 'idubica1' with random and unique values
        empty_indices = saldo_inv_cliente_fact[saldo_inv_cliente_fact['idubica1'] == ''].index

        # Generate unique random values using random.sample(), which guarantees uniqueness
        unique_values = random.sample(range(1, 1000000), len(empty_indices))

        # Convert to strings and assign back to the empty slots
        unique_values = [str(value) for value in unique_values]
        saldo_inv_cliente_fact.loc[empty_indices, 'idubica1'] = unique_values

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        inflow_with_mode_clean = saldo_inv_cliente_fact.dropna(subset=['idmodelo', 'idubica1'])

        # Step 1: Group by 'idmodelo' and 'idubica1' to count occurrences
        grouped_by_idubica1 = inflow_with_mode_clean[saldo_inv_cliente_fact['idubica1'].notna()].groupby(
            ['idmodelo', 'idubica1']).size().reset_index(name='count')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 2: Find the mode of the count for each idmodelo
        mode_grouping = grouped_by_idubica1.groupby('idmodelo')['count'].agg(lambda x: x.mode()[0]).reset_index(
            name='mode_count')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 3: Merge mode count with the inflow data (resumen_mensual_ingresos_fact)
        inflow_with_mode = pd.merge(resumen_mensual_ingresos_fact, mode_grouping, on='idmodelo', how='left')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 4: Fill missing mode_count with a default value (e.g., 1 if no grouping is available)
        inflow_with_mode['mode_count'] = inflow_with_mode['mode_count'].fillna(1)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 5: Calculate the number of rows per idingreso and idmodelo (consider specific products within each ingreso)
        df_grouped = inflow_with_mode.groupby(['idingreso', 'idmodelo']).size().reset_index(name='num_rows')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 6: Merge the number of rows per idingreso and idmodelo back into the inflow_with_mode dataframe
        inflow_with_mode = pd.merge(inflow_with_mode, df_grouped, on=['idingreso', 'idmodelo'], how='left')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 7: Calculate the number of pallets by dividing the num_rows by mode_count
        inflow_with_mode['pallets'] = (inflow_with_mode['num_rows'] / inflow_with_mode['mode_count']).apply(np.ceil)

        # Step 8: Group by idingreso and idmodelo to get the correct number of pallets for each combination
        pallets_per_ingreso = inflow_with_mode.groupby(['idingreso', 'idmodelo'])['pallets'].first().reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 9: Merge the pallet count back to the original dataframe
        inflow_with_mode = pd.merge(inflow_with_mode, pallets_per_ingreso[['idingreso', 'idmodelo', 'pallets']],
                                    on=['idingreso', 'idmodelo'], how='left', suffixes=('', '_final'))

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        inflow_with_mode['inicial'] = pd.to_numeric(inflow_with_mode['inicial'], errors='coerce')
        inflow_with_mode['pallets_final'] = pd.to_numeric(inflow_with_mode['pallets_final'], errors='coerce').astype(
            'Int64')
        inflow_with_mode['pallet_oficial'] = pd.to_numeric(inflow_with_mode.get('pallet_oficial', np.nan),
                                                           errors='coerce')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 1: Define a function to apply the conditional logic
        def choose_pallets(row):
            # If pallet_oficial is available, use it; otherwise, use pallets_final
            return row['pallet_oficial'] if not pd.isna(row['pallet_oficial']) else row['pallets_final']

        # Step 2: Apply the custom function
        inflow_with_mode['pallets_final'] = inflow_with_mode.apply(choose_pallets, axis=1)

        # Ensure all values in 'ddma' are numeric, and replace any non-numeric values with NaN
        inflow_with_mode['ddma'] = pd.to_numeric(inflow_with_mode['ddma'], errors='coerce')

        # Fill NaNs in 'ddma' with empty strings
        inflow_with_mode['ddma'] = inflow_with_mode['ddma'].fillna(0.0)

        # # Print unique values of 'ddma' after filling NaNs
        # print("Unique values in 'ddma' after fillna:")
        # print(inflow_with_mode['ddma'].unique())

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Define a function to adjust the pallet count for each group
        def adjust_pallets_final(group):
            # Count the number of rows with 'ddma' > 0
            num_splits = (group['ddma'] > 0).sum()

            # Subtract the number of splits from the initial pallet count (applies to all rows in the group)
            adjusted_pallets = group['pallets'].iloc[0] - num_splits

            # Assign the adjusted value to 'pallets_final' for all rows in the group
            group['pallets_final'] = adjusted_pallets

            return group

        # Group by 'idingreso' and 'idmodelo' and apply the adjustment function
        inflow_with_mode = inflow_with_mode.groupby(['idingreso', 'idmodelo'], group_keys=False).apply(
            adjust_pallets_final)

        # Reset the index after applying the group operation to ensure grouping columns are retained
        if 'idingreso' not in inflow_with_mode.columns or 'idmodelo' not in inflow_with_mode.columns:
            inflow_with_mode = inflow_with_mode.reset_index()

        # Ensure that the pallet count is not less than zero after adjustment
        inflow_with_mode['pallets_final'] = inflow_with_mode['pallets_final'].clip(lower=0)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Keep the historical df for further purposes.
        inflow_with_mode_historical = inflow_with_mode

        # Filter data within the date range
        inflow_with_mode = inflow_with_mode[
            (inflow_with_mode['fecha_x'] >= start_date) &
            (inflow_with_mode['fecha_x'] <= end_date)
            ]

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 3: Rename columns as needed
        inflow_with_mode = inflow_with_mode.rename(columns={
            'fecha_x': 'Date',
            'descrip': 'Description',
            'Bodega': 'Warehouse',
            'inicial': 'CBM',
            'pesokgs': 'Weight or Units',
            'pallets_final': 'Pallets'
        })

        # Ensure 'idingreso' is not part of the index before performing groupby
        if 'idingreso' in inflow_with_mode.index.names:
            inflow_with_mode.reset_index(drop=True, inplace=True)

            # Step:
            time.sleep(1)  # Simulate a task
            progress.update(task, advance=1)

        # Step 4: Final grouping by 'idingreso' and 'idmodelo' to aggregate relevant columns
        inflow_grouped = inflow_with_mode.groupby(['idingreso', 'idmodelo']).agg({
            'Date': 'first',
            'Description': 'first',
            'CBM': 'sum',
            'Pallets': 'min',
            'Weight or Units': 'sum',
            'Warehouse': 'first'

        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # # Final step: Write the cleaned outflow data to CSV or display as needed
        # output_path = os.path.join(get_base_output_path(), 'final_inflow_df_fact.csv')
        # inflow_grouped.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        # *** ACTUAL INVENTORY DATAFRAME ***

        # Step 1: Filter rows where 'idubica1' is not blank, not NaN, and does not start with 'R'
        filtered_df = saldo_inv_cliente_fact[
            saldo_inv_cliente_fact['idubica1'].notna() &  # Not NaN
            (saldo_inv_cliente_fact['idubica1'] != '') &  # Not blank
            ~saldo_inv_cliente_fact['idubica1'].str.startswith('R') &  # Does not start with 'R'
            ~saldo_inv_cliente_fact['idubica1'].str.startswith('TM')  # Does not start with 'R'
            ]

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # output_path = os.path.join(get_base_output_path(), 'saldo_inv_cliente_fact.csv')
        # saldo_inv_cliente_fact.to_csv(output_path, index=False)
        # output_path = os.path.join(get_base_output_path(), 'filtered_df.csv')
        # filtered_df.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 2: Create a 'pallets' column with value 1 for each row
        filtered_df['pallets'] = 1

        if 'dup_key' not in filtered_df.columns:
            filtered_df['dup_key'] = filtered_df['idingreso'] + filtered_df['itemno']


        # Step 3: Group by 'idubica1' and aggregate columns
        grouped_df = filtered_df.groupby('idubica1').agg({
            'idcentro': 'first',
            'idbodega': 'first',
            'idingreso': 'first',
            'itemno': 'last',
            'idstatus': 'first',
            'fecha': 'first',
            'idcontacto': 'first',
            'idubica': 'first',
            'pesokgs': 'sum',  # Summing numerical columns
            'inicial': 'sum',  # Summing 'inicial'
            'salidas': 'sum',  # Summing 'salidas'
            'bodega': 'first',
            'dup_key': 'first',
            'pallets': 'count'  # Summing 'pallets', which should now be 1 for each row
        }).reset_index()

        # Step 4: Get the rows where 'idubica1' is blank or starts with 'R'
        remaining_df = saldo_inv_cliente_fact[
            saldo_inv_cliente_fact['idubica1'].isna() |
            (saldo_inv_cliente_fact['idubica1'] == '') |
            saldo_inv_cliente_fact['idubica1'].str.startswith('R') &  # Does not start with 'R'
            saldo_inv_cliente_fact['idubica1'].str.startswith('TM')  # Does not start with 'R'
            ]

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 5: Assign 'pallets' = 1 for rows without 'idubica1' or starting with 'R'
        remaining_df['pallets'] = 1

        # Step 6: Concatenate the grouped rows with the remaining rows
        final_df = pd.concat([grouped_df, remaining_df]).reset_index(drop=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 7: Group by 'idingreso' and aggregate columns (final)
        final_df = final_df.groupby('idingreso').agg({
            'itemno': 'first',
            'dup_key': 'first',
            'fecha': 'first',
            'idcontacto': 'first',
            'idubica': 'first',
            'inicial': 'sum',  # Summing 'inicial'
            'pallets': 'count',
            'pesokgs': 'sum',  # Summing numerical columns
            'bodega': 'first',
        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Add the 'Days' column that calculates the number of days from the 'Date' to the current date
        final_df['fecha'] = pd.to_datetime(final_df['fecha']).dt.date
        final_df['Days'] = (datetime.now().date() - pd.to_datetime(final_df['fecha']).dt.date).apply(
            lambda x: x.days) + 1

        final_df = final_df.rename(columns={
            'fecha': 'Date',
            'idubica': 'locationID',
            'idubica1': 'Tarima',
            'inicial': 'CBM',
            'pesokgs': 'Weight or Units',
            'bodega': 'Warehouse',
            'idcontacto': 'ClientID',
            'idcoldis': 'ProductID',
            'pallets': 'Pallets',
            'dup_key': 'Label'

        })

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # output_path = os.path.join(get_base_output_path(), 'final_inventory_dataframe.csv')
        # final_df.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        # *** OUTFLOW CBM AND PALLETS ***

        # Step 1: Drop rows with NaN or missing values for 'idmodelo' and 'idubica1' in saldo_inv_cliente_fact
        saldo_clean = saldo_inv_cliente_fact.dropna(subset=['idmodelo', 'idubica1'])

        # Step 2: Filter rows in 'saldo_inv_cliente_fact' where 'idubica1' is not blank, NaN, or starting with 'R'
        filtered_saldo = saldo_clean[saldo_clean['idubica1'].notna() &
                                     (saldo_clean['idubica1'] != '') &
                                     ~saldo_clean['idubica1'].str.startswith('R') &  # Does not start with 'R'
                                     ~saldo_inv_cliente_fact['idubica1'].str.startswith(
                                         'TM')]  # Does not start with 'R'



        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 3: Group by 'idmodelo' and 'idubica1' to count occurrences in saldo_inv_cliente_fact
        grouped_by_idubica1_saldo = filtered_saldo.groupby(['idmodelo', 'idubica1']).size().reset_index(name='count')

        # output_path = os.path.join(get_base_output_path(), 'grouped_by_idubica1_saldo.csv')
        # grouped_by_idubica1_saldo.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 4: Find the mode of the count for each 'idmodelo'
        mode_grouping_saldo = grouped_by_idubica1_saldo.groupby('idmodelo')['count'].agg(
            lambda x: x.mode()[0]).reset_index(
            name='mode_count')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 5: Merge the mode count with the outflow data (resumen_despachos_cliente_fact) using 'idmodelo_x'
        outflow_with_mode = pd.merge(resumen_despachos_cliente_fact, mode_grouping_saldo, left_on='idmodelo_x',
                                     right_on='idmodelo', how='left')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 6: Fill missing 'mode_count' with a default value (e.g., 1 if no grouping is available)
        outflow_with_mode['mode_count'] = outflow_with_mode['mode_count'].fillna(1)

        """
        Returns the appropriate file path to the 'pallet_mode_KC.xlsx' depending on the operating system.
        """
        if os.name == 'nt':  # Windows
            unique_modes_file_path = None
            #unique_modes_file_path = r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\assets\'
            #inventory_analysis_client\pallet_mode_KC.xlsx'
            #return unique_modes_file_path

        else:  # macOS or others
            hostname = socket.gethostname()
            if 'JM-MS.local' in hostname:  # For Mac Studio
                unique_modes_df = pd.read_excel(
                    r'/Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/assets/'
                    r'inventory_analysis_client/pallet_mode_KC.xlsx')
            elif 'MacBook-Pro.local' in hostname:  # For MacBook Pro
                unique_modes_df = pd.read_excel(
                    r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/assets/'
                    r'inventory_analysis_client/pallet_mode_KC.xlsx')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Get the unique idmodelo
        unique_modes_df['idmodelo'] = unique_modes_df[['idmodelo']].drop_duplicates()

        # Ensure 'idmodelo' is a string
        unique_modes_df['idmodelo'] = unique_modes_df['idmodelo'].astype(str).str.strip()

        # Merge 'unique_modes_df' into 'outflow_with_mode' on 'idmodelo'
        outflow_with_mode = pd.merge(
            outflow_with_mode,
            unique_modes_df[['idmodelo', 'mode_count']],
            left_on='idmodelo_x',
            right_on='idmodelo',
            how='left',
            suffixes=('_existing', '_new')
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Now, compare 'mode_count_existing' and 'mode_count_new', and if 'mode_count_new' > 'mode_count_existing', replace 'mode_count_existing' with 'mode_count_new'
        # First, ensure 'mode_count_existing' and 'mode_count_new' are numeric
        outflow_with_mode['mode_count_existing'] = pd.to_numeric(outflow_with_mode['mode_count_existing'],
                                                                 errors='coerce')
        outflow_with_mode['mode_count_new'] = pd.to_numeric(outflow_with_mode['mode_count_new'], errors='coerce')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Where 'mode_count_new' > 'mode_count_existing', replace 'mode_count_existing' with 'mode_count_new'
        condition = outflow_with_mode['mode_count_new'] > outflow_with_mode['mode_count_existing']
        outflow_with_mode.loc[condition, 'mode_count_existing'] = outflow_with_mode.loc[condition, 'mode_count_new']

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Now, drop 'mode_count_new' and rename 'mode_count_existing' back to 'mode_count'
        outflow_with_mode.drop(columns=['mode_count_new', 'idmodelo_y'], inplace=True)
        outflow_with_mode.rename(columns={'mode_count_existing': 'mode_count'}, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 7: Calculate the number of rows per 'trannum' and 'idmodelo_x'
        df_grouped_outflow = outflow_with_mode.groupby(['trannum', 'idmodelo_x']).size().reset_index(name='num_rows')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 8: Merge the number of rows back into the outflow_with_mode dataframe
        outflow_with_mode = pd.merge(outflow_with_mode, df_grouped_outflow, on=['trannum', 'idmodelo_x'], how='left')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Ensure num_rows and mode_count are numeric
        outflow_with_mode['num_rows'] = pd.to_numeric(outflow_with_mode['num_rows'], errors='coerce')
        outflow_with_mode['mode_count'] = pd.to_numeric(outflow_with_mode['mode_count'], errors='coerce')

        # Step 9: Calculate the number of pallets by dividing 'num_rows' by 'mode_count'
        # Use np.ceil() to round up after the division
        outflow_with_mode['pallets'] = np.ceil(outflow_with_mode['num_rows'] / outflow_with_mode['mode_count'])

        # Step 10: Create a new column 'calculated_pallets' without rounding
        outflow_with_mode['calculated_pallets'] = outflow_with_mode['num_rows'] / outflow_with_mode['mode_count']

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 11: Fill missing or NaN 'idubica1' as 1 pallet
        outflow_with_mode.loc[outflow_with_mode['idubica1'].isna(), 'pallets'] = 1
        outflow_with_mode.loc[outflow_with_mode['idubica1'] == '', 'pallets'] = 1

        # Step 12: Round 'calculated_pallets' to the nearest integer
        outflow_with_mode['calculated_pallets'] = np.ceil(outflow_with_mode['calculated_pallets'])

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # output_path = os.path.join(get_base_output_path(), 'outflow_with_mode_before_merge.csv')
        # outflow_with_mode.to_csv(output_path, index=False)

        # Use lambda function to pad idingreso with leading zeros
        # outflow_with_mode['idingreso'] = outflow_with_mode['idingreso'].apply(
        #     lambda x: f"{int(x):010}" if pd.notna(x) else x)

        outflow_with_mode['idingreso'] = outflow_with_mode['idingreso'].apply(
            lambda x: f"{int(x):010}" if pd.notna(x) and str(x).isnumeric() else x
        )

        registro_ingresos['idingreso'] = registro_ingresos['idingreso'].apply(
            lambda x: f"{int(x):010}" if pd.notna(x) and str(x).isnumeric() else x
        )

        # registro_ingresos['idingreso'] = registro_ingresos['idingreso'].apply(
        #     lambda x: f"{int(x):010}" if pd.notna(x) else x)

        # print("outflow_with_mode['idingreso'] unique values:\n", outflow_with_mode['idingreso'].unique())
        # print("registro_ingresos['idingreso'] unique values:\n", registro_ingresos['idingreso'].unique())

        duplicates = registro_ingresos[registro_ingresos.duplicated(subset='idingreso', keep=False)]
        # print("Duplicate idingreso in registro_ingresos:\n", duplicates)

        # print("registro_ingresos['descrip'] sample values:\n", registro_ingresos['descrip'].head())
        missing_descrip = registro_ingresos[registro_ingresos['descrip'].isna()]
        # print(f"Rows in registro_ingresos with missing descrip: {len(missing_descrip)}")

        outflow_with_mode['idingreso'] = outflow_with_mode['idingreso'].astype(str).str.strip().str.zfill(10)
        registro_ingresos['idingreso'] = registro_ingresos['idingreso'].astype(str).str.strip().str.zfill(10)

        missing_keys = outflow_with_mode.loc[
            ~outflow_with_mode['idingreso'].isin(registro_ingresos['idingreso']), 'idingreso']
        # print("Missing idingreso values (not found in registro_ingresos):")
        # print(missing_keys.unique())

        # Merge with registro_ingresos to get OC description.
        outflow_with_mode = pd.merge(outflow_with_mode, registro_ingresos[['idingreso', 'descrip']], on='idingreso',
                                     how="left", indicator=True)

        # Replace None or NaN values in 'descrip' column with 'Unknown'
        outflow_with_mode['descrip'] = outflow_with_mode['descrip'].fillna('Unknown')

        # Rename 'idcontacto_x' to 'idcontacto' for consistency
        outflow_with_mode.rename(columns={'idcontacto_x': 'idcontacto'}, inplace=True)

        #Merge with supplier info to get Client name.
        outflow_with_mode = pd.merge(outflow_with_mode, supplier_info[['idcontacto', 'Client']], on= 'idcontacto',
                                     how = 'left')

        # Rename 'idcontacto_x' to 'idcontacto' for consistency
        outflow_with_mode.rename(columns={'idcontacto_x': 'idcontacto'}, inplace=True)

        # output_path = os.path.join(get_base_output_path(), 'outflow_with_mode_after_merge.csv')
        # outflow_with_mode.to_csv(output_path, index=False)
        #
        # output_path = os.path.join(get_base_output_path(), 'registro_ingresos_test.csv')
        # registro_ingresos.to_csv(output_path, index=False)

        # # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Drop duplicates based on 'dup_key'
        outflow_with_mode = outflow_with_mode.drop_duplicates(subset='dup_key')

        # Keep fecha_x and fecha_y as Timestamps
        outflow_with_mode['fecha_x'] = pd.to_datetime(outflow_with_mode['fecha_x'])
        outflow_with_mode['fecha_y'] = pd.to_datetime(outflow_with_mode['fecha_y'])

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate the number of days
        outflow_with_mode['Days'] = abs((outflow_with_mode['fecha_x'] - outflow_with_mode['fecha_y']).dt.days) + 1

        # Keep the historical df for further purposes.
        outflow_with_mode_historical = outflow_with_mode

        # Filter by date
        outflow_with_mode = outflow_with_mode[
            (outflow_with_mode['fecha_x'] >= start_date) &
            (outflow_with_mode['fecha_x'] <= end_date)
            ]

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 13: Group by 'trannum' and 'idmodelo_x' and perform the final aggregations
        outflow_grouped = outflow_with_mode.groupby(['trannum', 'idmodelo_x']).agg({
            'fecha_x': 'first',  # First occurrence of 'fecha_x'
            'fecha_y': 'last',
            'Days': 'mean',
            'descrip': 'first',  # Purchase Order
            'cantidad': 'sum',  # Sum of 'cantidad'
            'pallets': 'first',
            'pesokgs': 'sum',
            'calculated_pallets': 'first',
            'bodega': 'first',
            'idcontacto': 'first',
            'Client': 'first'
        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Round 'Days' to 2 decimal places
        outflow_grouped['Days'] = outflow_grouped['Days'].round(2)

        # Step 14: Drop the unnecessary columns
        outflow_grouped = outflow_grouped.drop(columns=['pallets'])

        # Step 15: Round 'calculated_pallets' to the nearest integer
        outflow_grouped['calculated_pallets'] = np.ceil(outflow_grouped['calculated_pallets']).astype('Int64')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 16: Rename columns for the final output
        outflow_grouped = outflow_grouped.rename(columns={
            'fecha_x': 'Shipping_Date',
            'fecha_y': 'Arrival_Date',
            'pesokgs': 'Weight or Units',
            'calculated_pallets': 'Pallets',
            'cantidad': 'CBM',
            'idmodelo_x': 'idmodelo',
            # 'idcontacto_x': 'idcontacto',
            'descrip': 'Description',
            'bodega': 'Warehouse'
        })

        # print("\noutflow_grouped:\n", outflow_grouped.head())


        outflow_grouped = outflow_grouped.loc[:,
                          ['trannum', 'idmodelo', 'Arrival_Date', 'Shipping_Date', 'Days', 'Description', 'idcontacto',
                           'Client','CBM',
                           'Pallets',
                           'Weight or Units',
                           'Warehouse']]

        # # Write the cleaned outflow data to CSV
        # output_path = os.path.join(get_base_output_path(), 'final_outflow_df_fact.csv')
        # outflow_grouped.to_csv(output_path, index=False)

        # output_path = os.path.join(get_base_output_path(), 'inflow_with_mode_historical.csv')
        # inflow_with_mode_historical.to_csv(output_path, index=False)
        # output_path = os.path.join(get_base_output_path(), 'outflow_with_mode_historical.csv')
        # outflow_with_mode_historical.to_csv(output_path, index=False)
        #
        # output_path = os.path.join(get_base_output_path(), 'inflow_with_mode.csv')
        # inflow_with_mode.to_csv(output_path, index=False)
        # output_path = os.path.join(get_base_output_path(), 'outflow_with_mode.csv')
        # outflow_with_mode.to_csv(output_path, index=False)
        #
        # output_path = os.path.join(get_base_output_path(), 'final_df.csv')
        # final_df.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        total_inflow_pallets = inflow_grouped['Pallets'].sum()
        total_inflow_cbm = inflow_grouped['CBM'].sum()

        total_outflow_pallets = outflow_grouped['Pallets'].sum()
        total_outflow_cbm = outflow_grouped['CBM'].sum()

        total_pallets_inv = final_df['Pallets'].sum()
        total_cbm_inventory = final_df['CBM'].sum()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("\n Total Pallets and CBM count:\n")
    print("Pallets received:\n", total_inflow_pallets)
    print("CBM received:\n", total_inflow_cbm)

    print("Pallets shipped:\n", total_outflow_pallets)
    print("CBM shipped:\n", total_outflow_cbm)

    print("Pallets on inventory - Actual:\n", total_pallets_inv)
    print("CBM on inventory - Actual:\n", total_cbm_inventory)

    # Display the final dataframes
    print("\nFinal Inflow dataframe:\n", inflow_grouped)
    print("\nFinal Outflow DataFrame:\n", outflow_grouped)
    print("\nFinal inventory dataframe:\n", final_df)

    print("Clients Operational data reconstructed successfully.\n")

    return inflow_with_mode_historical, outflow_with_mode_historical, final_df

