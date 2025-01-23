from rich.progress import Progress
import pandas as pd
import time
import numpy as np



def kpi_calculation(inventory_over_time, inventory_ot_by_month, start_date, end_date):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Constructing KPIs: ", total=9)

        # Ensure the 'date' column is in datetime format
        inventory_over_time['date'] = pd.to_datetime(inventory_over_time['date'])

        # # Filter data within the date range
        # inventory_over_time = inventory_over_time[
        #     (inventory_over_time['date'] >= start_date) & (inventory_over_time['date'] <= end_date)
        #     ]

        # Add 'month' column for grouping
        inventory_over_time['month'] = inventory_over_time['date'].dt.to_period('M')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Aggregate data by month
        monthly_data = inventory_over_time.groupby('month').agg({
            'Inflow (CBM)': 'sum',
            'Outflow (CBM)': 'sum',
            'Inventory level (CBM)': 'last'  # Use last inventory level of the month
        }).reset_index()

        # Calculate Average Inventory Level per month
        monthly_data['Average Inventory Level (CBM)'] = (
                                                                monthly_data['Inventory level (CBM)'] + monthly_data[
                                                            'Inventory level (CBM)'].shift(1)
                                                        ) / 2

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate Inventory Turnover per month
        monthly_data['Inventory Turnover'] = monthly_data.apply(
            lambda row: (row['Outflow (CBM)'] / row['Average Inventory Level (CBM)'])
            if row['Average Inventory Level (CBM)'] != 0 else np.nan,
            axis=1
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Replace infinite values with NaN
        monthly_data['Inventory Turnover'].replace([np.inf, -np.inf], np.nan, inplace=True)

        # Calculate Days on Hand per month
        monthly_data['Days on Hand'] = monthly_data.apply(
            lambda row: (row['Inventory level (CBM)'] / (row['Outflow (CBM)'] / row['month'].asfreq('M').days_in_month))
            if row['Outflow (CBM)'] != 0 else np.nan,
            axis=1
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate MoM Percentage Changes for KPIs
        monthly_data['Inflow MoM %'] = monthly_data['Inflow (CBM)'].pct_change() * 100
        monthly_data['Outflow MoM %'] = monthly_data['Outflow (CBM)'].pct_change() * 100
        monthly_data['Inventory Level MoM %'] = monthly_data['Inventory level (CBM)'].pct_change() * 100

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Replace infinite values with NaN
        for col in ['Inflow MoM %', 'Outflow MoM %', 'Inventory Level MoM %']:
            monthly_data[col].replace([np.inf, -np.inf], np.nan, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Fill NaN values where appropriate
        monthly_data.fillna({
            'Inventory Turnover': 0,
            'Days on Hand': 0,
            'Inflow MoM %': 0,
            'Outflow MoM %': 0,
            'Inventory Level MoM %': 0,
        }, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Round numerical values for presentation
        monthly_data = monthly_data.round(2)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Replace any remaining NaN values with 'N/A' for clarity
        monthly_data.replace({np.nan: 'N/A'}, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Convert 'month' back to string format for better display
        monthly_data['month'] = monthly_data['month'].astype(str)

        # Filter the data to include only months within the date range
        # Since months are in 'YYYY-MM' format, we'll adjust start and end dates accordingly
        start_month = start_date.to_period('M').strftime('%Y-%m')
        end_month = end_date.to_period('M').strftime('%Y-%m')
        # monthly_data = monthly_data[
        #     (monthly_data['month'] >= start_month) & (monthly_data['month'] <= end_month)
        #     ]

        # Reorder columns for better presentation
        monthly_data = monthly_data[[
            'month',
            'Inflow (CBM)',
            'Outflow (CBM)',
            'Inventory level (CBM)',
            'Inventory Turnover',
            'Days on Hand',
            'Inflow MoM %',
            'Outflow MoM %',
            'Inventory Level MoM %',
        ]]

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    # Print KPIs for the selected months
    print("\nKPIs for the selected months:\n", monthly_data)
    print("\nKPI calculation complete.\n")

    return monthly_data

