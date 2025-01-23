from rich.progress import Progress
import time
import pandas as pd
from utils import clip_near_zero
import os
from utils import get_base_output_path

def reconstruct_inventory_over_time(
        inflow_with_mode_historical,
        outflow_with_mode_historical,
        start_date=None,
        end_date=None,
        initial_inventory=None
):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Reconstructing inventory behavior Data: ", total=18)

        # Ensure 'idingreso' and 'itemno' are strings
        inflow_with_mode_historical['idingreso'] = inflow_with_mode_historical['idingreso'].astype(str)
        inflow_with_mode_historical['itemno'] = inflow_with_mode_historical['itemno'].astype(str)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Create 'dup_key' in resumen_mensual_ingresos_sd
        inflow_with_mode_historical['dup_key'] = inflow_with_mode_historical['idingreso'] + inflow_with_mode_historical[
            'itemno']

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # print("Outlflow with mode historicall:\n", outflow_with_mode_historical)

        # Ensure 'idingreso' is a string
        outflow_with_mode_historical['idingreso'] = outflow_with_mode_historical['idingreso'].astype(str)

        # Check for the presence of 'itemno' or 'itemno_x' and handle accordingly
        if 'itemno' in outflow_with_mode_historical.columns:
            outflow_with_mode_historical['itemno'] = outflow_with_mode_historical['itemno'].astype(str)
            outflow_with_mode_historical['dup_key'] = (
                    outflow_with_mode_historical['idingreso'] + outflow_with_mode_historical['itemno']
            )
        elif 'itemno_x' in outflow_with_mode_historical.columns:
            outflow_with_mode_historical['itemno_x'] = outflow_with_mode_historical['itemno_x'].astype(str)
            outflow_with_mode_historical['dup_key'] = (
                    outflow_with_mode_historical['idingreso'] + outflow_with_mode_historical['itemno_x']
            )
        else:
            # If neither 'itemno' nor 'itemno_x' is present, create a placeholder for 'dup_key'
            outflow_with_mode_historical['dup_key'] = outflow_with_mode_historical['idingreso']

        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Ensure date columns are in datetime format and normalize to remove time component
        inflow_with_mode_historical['fecha_x'] = pd.to_datetime(
            inflow_with_mode_historical['fecha_x'], errors='coerce'
        ).dt.normalize()
        outflow_with_mode_historical['fecha_x'] = pd.to_datetime(
            outflow_with_mode_historical['fecha_x'], errors='coerce'
        ).dt.normalize()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Drop rows with invalid or missing dates
        inflow_with_mode_historical.dropna(subset=['fecha_x'], inplace=True)
        outflow_with_mode_historical.dropna(subset=['fecha_x'], inplace=True)

        # Step 2: Determine Earliest and Latest Dates if not provided
        if start_date is None:
            start_date = min(
                inflow_with_mode_historical['fecha_x'].min(),
                outflow_with_mode_historical['fecha_x'].min()
            ).date()
        else:
            start_date = pd.to_datetime(start_date).date()

        if end_date is None:
            end_date = max(
                inflow_with_mode_historical['fecha_x'].max(),
                outflow_with_mode_historical['fecha_x'].max()
            ).date()
        else:
            end_date = pd.to_datetime(end_date).date()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 3: Prepare date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # Filter dates to include only those with transactions
        valid_dates = pd.concat([
            inflow_with_mode_historical[['fecha_x']],
            outflow_with_mode_historical[['fecha_x']]
        ]).drop_duplicates().rename(columns={'fecha_x': 'date'})

        date_df = pd.DataFrame({'date': date_range}).merge(valid_dates, on='date', how='inner')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        print("Pallets final check (inflow):\n", inflow_with_mode_historical)
        print("Pallets final check (outflow):\n", outflow_with_mode_historical)

        # Aggregate daily inflows
        daily_inflows = inflow_with_mode_historical.groupby(['fecha_x', 'idcontacto']).agg({
            'inicial': 'sum',
            'pesokgs': 'sum',
            'pallets_final': 'sum'  # no. de palets
        }).reset_index()

        daily_inflows.rename(columns={
            'fecha_x': 'date',
            'inicial': 'Inflow (CBM)',
            'pesokgs': 'Units inflow',
            'pallets_final': 'Pallets inflow'
        }, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Aggregate daily outflows
        daily_outflows = outflow_with_mode_historical.groupby(['fecha_x', 'idcontacto']).agg({
            'cantidad': 'sum',
            'pesokgs': 'sum',
            'calculated_pallets': 'sum'
        }).reset_index()

        daily_outflows.rename(columns={
            'fecha_x': 'date',
            'cantidad': 'Outflow (CBM)',
            'pesokgs': 'Units outflow',
            'calculated_pallets': 'Pallets outflow'
        }, inplace=True)

        # Prepare clients list
        clients = pd.concat([
            daily_inflows[['idcontacto']].drop_duplicates(),
            daily_outflows[['idcontacto']].drop_duplicates()
        ]).drop_duplicates()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Cross join clients with date range
        inventory_over_time = clients.merge(date_df, how='cross')

        # Merge inflows and outflows
        inventory_over_time = inventory_over_time.merge(
            daily_inflows,
            on=['date', 'idcontacto'],
            how='left'
        ).merge(
            daily_outflows,
            on=['date', 'idcontacto'],
            how='left'
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Fill NaNs with zeros for inflow and outflow columns
        inventory_over_time['Inflow (CBM)'] = inventory_over_time['Inflow (CBM)'].fillna(0)
        inventory_over_time['Outflow (CBM)'] = inventory_over_time['Outflow (CBM)'].fillna(0)
        inventory_over_time['Units inflow'] = inventory_over_time['Units inflow'].fillna(0)
        inventory_over_time['Units outflow'] = inventory_over_time['Units outflow'].fillna(0)
        inventory_over_time['Pallets inflow'] = inventory_over_time['Pallets inflow'].fillna(0)
        inventory_over_time['Pallets outflow'] = inventory_over_time['Pallets outflow'].fillna(0)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate cumulative inventory levels per client
        inventory_over_time = inventory_over_time.sort_values(['idcontacto', 'date'])
        inventory_over_time['Inventory level (CBM)'] = 0.0

        # Set initial inventory levels
        if initial_inventory is None:
            initial_inventory = pd.DataFrame({
                'idcontacto': clients['idcontacto'],
                'initial_inventory': 0.0
            })

        # Merge initial inventory
        inventory_over_time = inventory_over_time.merge(
            initial_inventory,
            on='idcontacto',
            how='left'
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        df_client_share = (
            inventory_over_time
            .groupby('idcontacto', as_index=False)
            .agg({'Inflow (CBM)': 'sum', 'Outflow (CBM)': 'sum'})
        )

        # Compute percentages
        total_inflow = df_client_share['Inflow (CBM)'].sum()
        df_client_share['Inflow %'] = df_client_share['Inflow (CBM)'] / total_inflow * 100

        inventory_ot_by_month = inventory_over_time

        # 1. Group by date only, summing relevant numeric columns you care about:
        daily_agg = (
            inventory_over_time
            .groupby('date', as_index=False)
            .agg({
                'Inflow (CBM)': 'sum',
                'Outflow (CBM)': 'sum',
                # If you also track 'Units inflow', 'Units outflow', etc., include them here:
                'Units inflow': 'sum',
                'Pallets inflow': 'sum',
                'Units outflow': 'sum',
                'Pallets outflow': 'sum',
                # If you want to incorporate any initial_inventory from each row:
                'initial_inventory': 'sum'
            })
            .sort_values('date')  # Ensure ascending chronological order
        )

        # 2. Create an 'Inventory level (CBM)' column (start it at 0 or the sum of initial_inventory on the first day).
        daily_agg['Inventory level (CBM)'] = 0.0

        # If you have a reason to set the very first day's starting point from the sum of 'initial_inventory', do:
        if not daily_agg.empty:
            current_inventory = daily_agg.loc[daily_agg.index[0], 'initial_inventory']
        else:
            current_inventory = 0.0

        # 3. Calculate the daily running total, preventing negative values.
        inventory_levels = []
        for idx, row in daily_agg.iterrows():
            inflow = row['Inflow (CBM)']
            outflow = row['Outflow (CBM)']

            new_inventory = current_inventory + inflow - outflow
            if new_inventory < 0:
                new_inventory = 0
            inventory_levels.append(new_inventory)

            current_inventory = new_inventory

        daily_agg['Inventory level (CBM)'] = inventory_levels

        inventory_over_time = daily_agg.copy()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Compute Opening Inventory level (CBM)
        inventory_over_time['Opening Inventory level (CBM)'] = (
                inventory_over_time['Inventory level (CBM)'] - inventory_over_time['Inflow (CBM)'] +
                inventory_over_time['Outflow (CBM)']
        )

        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Group by month and calculate initial and final inventory levels
        inventory_ot_by_month = inventory_ot_by_month.groupby(pd.Grouper(key='date', freq='M')).agg({
            'idcontacto': 'first',
            'Inflow (CBM)': 'sum',
            'Units inflow': 'sum',
            'Pallets inflow': 'sum',
            'Outflow (CBM)': 'sum',
            'Units outflow': 'sum',
            'Pallets outflow': 'sum',
            # 'Opening Inventory level (CBM)': 'first',  # Initial inventory
            'Inventory level (CBM)': 'last',  # Final inventory
        }).reset_index()

        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns
        inventory_ot_by_month.rename(columns={
            # 'Opening Inventory level (CBM)': 'Initial Inventory level (CBM)',
            'Inventory level (CBM)': 'Final Inventory level (CBM)'
        }, inplace=True)

        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        print("Clients contained in analysis:\n", df_client_share.sort_values('Inflow (CBM)', ascending=False))

        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        inventory_over_time = (
            inventory_over_time
            .groupby(['date'], as_index=False)
            .agg({
                'Inflow (CBM)': 'sum',
                'Units inflow': 'sum',
                'Pallets inflow': 'sum',
                'Outflow (CBM)': 'sum',
                'Units outflow': 'sum',
                'Pallets outflow': 'sum',
                # For inventory level, let's keep the last recorded value that day
                'Inventory level (CBM)': 'last',
                # If 'initial_inventory' is relevant, you can do 'first'
                'initial_inventory': 'first',
            })
        )

        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        inventory_ot_by_month.rename(columns={
            'initial_inventory': 'Initial Inventory level (CBM)',
        }, inplace=True)

        # Step 2: Clip near-zero values
        inventory_over_time = clip_near_zero(inventory_over_time)

        # # Save to CSV
        # output_path = os.path.join(get_base_output_path(), 'insaldo_historic_dataframe_behavior_by_month.csv')
        # inventory_ot_by_month.to_csv(output_path, index=False)
        # output_path = os.path.join(get_base_output_path(), 'inventory_over_time.csv')
        # inventory_over_time.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("\nInventory behavior reconstruction complete.\n")

    return inventory_over_time, inventory_ot_by_month
