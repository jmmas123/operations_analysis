from datetime import datetime

import numpy as np
import pandas as pd
import os


def get_base_output_path():
    if os.name == 'nt':  # Windows
        obase_path = r'C:\Users\josemaria\Downloads'
    else:  # MacOS (or others)
        obase_path = r'/Users/j.m./Downloads'
    return obase_path


def load_data(start_date=None, end_date=None):
    # Define the paths to your data files
    def get_base_path(file_type):
        if os.name == 'nt':  # Windows
            if file_type == 'overtime_t':
                return r'C:\JM\GM\MOBU - OPL\horas extra'
            elif file_type == 'overtime':
                return r'\\192.168.10.18\Bodega General\HE\VARIOS\Horas'
            elif file_type == 'workforce':
                return r'C:\JM\GM\MOBU - OPL\Planilla'
        else:  # MacOS
            if file_type == 'overtime':
                return (r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - '
                        r'OPL/Horas extra')
            if file_type == 'overtime_t':
                return '/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Horas extra'
            elif file_type == 'workforce':
                return '/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Planilla'

    # Get base paths
    overtime_t_base_path = get_base_path('overtime_t')
    overtime_base_path = get_base_path('overtime')
    workforce_base_path = get_base_path('workforce')

    # Construct file paths
    overtime_file_path = os.path.join(overtime_base_path, 'Horas extra NF.xlsx')
    workforce_and_salaries_path = os.path.join(workforce_base_path, 'Reporte de personal MORIBUS.xlsx')
    income_overtime_client_path = os.path.join(overtime_t_base_path, 'tarifas_h_extra.xlsx')

    # Read data files
    df_warehouse = pd.read_excel(overtime_file_path, sheet_name='Horas en bodega', header=0, dtype={'Codigo': str,
                                                                                                    'Idcontacto': str})
    df_delivery = pd.read_excel(overtime_file_path, sheet_name='Horas en ruta', header=0, dtype={'Codigo': str})
    df_salary = pd.read_excel(workforce_and_salaries_path, sheet_name='Empleados (analisis de costos)', header=0)
    income_overtime_client = pd.read_excel(income_overtime_client_path, header=0)

    # Convert 'Fecha' columns to datetime to apply date filtering
    df_warehouse['Fecha'] = pd.to_datetime(df_warehouse['Fecha'], dayfirst=True)
    df_delivery['Fecha'] = pd.to_datetime(df_delivery['Fecha'], dayfirst=True)

    # Apply date filtering if start_date and end_date are provided
    if start_date:
        df_warehouse = df_warehouse[df_warehouse['Fecha'] >= start_date]
        df_delivery = df_delivery[df_delivery['Fecha'] >= start_date]
    if end_date:
        df_warehouse = df_warehouse[df_warehouse['Fecha'] <= end_date]
        df_delivery = df_delivery[df_delivery['Fecha'] <= end_date]

    # Additional data processing steps
    df_warehouse['Bodega'] = df_warehouse['Bodega'].str.upper()
    df_warehouse['Bodega'] = df_warehouse['Bodega'].astype(str).str.strip().str.upper()

    print("Filtered Warehouse dataframe:\n", df_warehouse.head())
    print("Filtered Delivery dataframe:\n", df_delivery.head())

    return df_delivery, df_warehouse, df_salary, income_overtime_client


def data_normalization(df_warehouse, df_delivery, df_salary, income_overtime_client):
    # Convert date columns to datetime with dayfirst=True
    df_warehouse['Fecha'] = pd.to_datetime(df_warehouse['Fecha'], dayfirst=True)
    df_delivery['Fecha'] = pd.to_datetime(df_delivery['Fecha'], dayfirst=True)

    # Fill NaN values in 'Nombre' column with 'Unknown'
    df_delivery['Nombre'].fillna('Unknown', inplace=True)
    df_warehouse['Nombre'].fillna('Unknown', inplace=True)

    # Remove the specified columns from df_salary
    df_salary = df_salary.drop(columns=['N°', 'Empresa', 'Nombre Completo', 'P. Ingreso', ])

    # Convert 'Codigo' columns to strings
    df_salary['Codigo'] = df_salary['Codigo'].astype(str)
    df_warehouse['Codigo'] = df_warehouse['Codigo'].astype(str)
    df_delivery['Codigo'] = df_delivery['Codigo'].astype(str)

    print("Warehouse normalized dataframe:\n", df_warehouse.head())
    print("Delivery normalized dataframe:\n", df_delivery.head())
    print("Salary normalized dataframe:\n", df_salary.head())
    print("Client normalized tarifs dataframe:\n", income_overtime_client.head())

    return df_delivery, df_warehouse, df_salary, income_overtime_client


def cost_calculator(df_delivery, df_warehouse, df_salary, start_date=None, end_date=None):
    # Filter by date if start_date and end_date are provided
    if start_date:
        df_delivery = df_delivery[df_delivery['Fecha'] >= start_date]
        df_warehouse = df_warehouse[df_warehouse['Fecha'] >= start_date]
    if end_date:
        df_delivery = df_delivery[df_delivery['Fecha'] <= end_date]
        df_warehouse = df_warehouse[df_warehouse['Fecha'] <= end_date]

    # Existing cost calculation logic goes here
    df_warehouse = pd.merge(df_salary, df_warehouse, on='Codigo')
    df_delivery = pd.merge(df_salary, df_delivery, on='Codigo')

    # Reorder columns in each dataframe
    ordered_columns = [
        'Fecha', 'Cliente', 'Idcontacto', 'Bodega', 'Nombre', 'Cargo', 'Codigo',
        'Descripcion', 'Observaciones', 'Hora de inicio', 'Hora de finalizacion',
        'Horas diurnas', 'Horas nocturnas', 'Total', 'Hora diurna', 'Hora nocturna', 'Hora domingo'
    ]
    df_warehouse = df_warehouse[ordered_columns]

    delivery_ordered_columns = [
        'Fecha', 'Cliente', 'Tipo', 'Ruta', 'Puntos de entrega', 'Puntos adicionales', 'Nombre', 'Cargo',
        'Codigo', 'Hora de inicio', 'Hora de finalizacion',
        'Horas diurnas', 'Horas nocturnas', 'Total', 'Aprobacion', 'Hora diurna', 'Hora nocturna', 'Hora domingo'
    ]
    df_delivery = df_delivery[delivery_ordered_columns]

    # Convert 'Fecha' to datetime to check for Sundays
    df_delivery['Fecha'] = pd.to_datetime(df_delivery['Fecha'], dayfirst=True)
    df_warehouse['Fecha'] = pd.to_datetime(df_warehouse['Fecha'], dayfirst=True)

    # Ensure overtime hour columns and rates are numeric
    for df in [df_delivery, df_warehouse]:
        df['Horas diurnas'] = pd.to_numeric(df['Horas diurnas'], errors='coerce')
        df['Hora diurna'] = pd.to_numeric(df['Hora diurna'], errors='coerce')
        df['Horas nocturnas'] = pd.to_numeric(df['Horas nocturnas'], errors='coerce')
        df['Hora nocturna'] = pd.to_numeric(df['Hora nocturna'], errors='coerce')
        df['Hora domingo'] = pd.to_numeric(df['Hora domingo'], errors='coerce')

        # Calculate costs based on day of the week
        df['Is_Sunday'] = df['Fecha'].dt.dayofweek == 6  # Sunday is represented by 6

        # Apply Sunday rate if Is_Sunday is True
        df['Horas diurnas ($)'] = np.where(
            df['Is_Sunday'],
            df['Horas diurnas'] * df['Hora domingo'],
            df['Horas diurnas'] * df['Hora diurna']
        )
        df['Horas nocturnas ($)'] = np.where(
            df['Is_Sunday'],
            df['Horas nocturnas'] * df['Hora domingo'],
            df['Horas nocturnas'] * df['Hora nocturna']
        )

        # Calculate the total cost column and fill NaN values with 0
        df['Horas diurnas ($)'] = df['Horas diurnas ($)'].fillna(0)
        df['Horas nocturnas ($)'] = df['Horas nocturnas ($)'].fillna(0)
        df['Total ($)'] = df['Horas diurnas ($)'] + df['Horas nocturnas ($)']
        df['Total ($)'] = df['Total ($)'].fillna(0)

    print("\nDelivery dataframe:\n", df_delivery)
    print("\nWarehouse dataframe:\n", df_warehouse)

    # Return the updated DataFrames
    return df_delivery, df_warehouse


def merge_intervals(intervals):
    """
    Merge overlapping intervals and return total duration in hours.
    """
    if not intervals:
        return 0.0
    # Sort intervals by start time
    intervals = sorted(intervals, key=lambda x: x[0])
    merged = []
    current_start, current_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= current_end:
            # Overlapping intervals, merge them
            current_end = max(current_end, end)
        else:
            # Non-overlapping interval, add the previous one and reset
            merged.append((current_start, current_end))
            current_start, current_end = start, end
    # Add the last interval
    merged.append((current_start, current_end))
    # Calculate total duration
    total_duration = sum((end - start).total_seconds() for start, end in merged) / 3600
    return total_duration


def adjust_overlapping_costs(df):
    # Ensure Fecha is datetime
    df['Fecha'] = pd.to_datetime(df['Fecha'])

    # Ensure 'Hora de inicio' and 'Hora de finalizacion' are strings
    df['Hora de inicio'] = df['Hora de inicio'].astype(str)
    df['Hora de finalizacion'] = df['Hora de finalizacion'].astype(str)

    # Combine Fecha with Hora de inicio and Hora de finalizacion
    df['Inicio'] = pd.to_datetime(
        df['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df['Hora de inicio'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )
    df['Fin'] = pd.to_datetime(
        df['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df['Hora de finalizacion'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )

    # Calculate task durations in hours
    df['Duracion'] = (df['Fin'] - df['Inicio']).dt.total_seconds() / 3600

    # Initialize columns for adjusted calculations
    df['Total h t'] = 0.0
    df['Total proporcional'] = 0.0
    df['Total Proporcional ($)'] = 0.0
    df['Horas Reales'] = 0.0
    df['Duracion Ajustada (H)'] = 0.0

    # Group by Codigo and Fecha
    grouped = df.groupby(['Codigo', 'Fecha'])

    for (codigo, fecha), group in grouped:
        tasks = group.reset_index()
        n_tasks = len(tasks)

        # Calculate real hours worked by merging intervals
        intervals = list(zip(group['Inicio'], group['Fin']))
        real_hours = merge_intervals(intervals)
        # Assign real_hours to all rows in group
        df.loc[group.index, 'Horas Reales'] = real_hours

        # Initialize list to hold overlapping groups
        overlapping_groups = []

        # Build overlapping groups
        for idx_i, task_i in tasks.iterrows():
            found_group = False
            for group_indices in overlapping_groups:
                for idx_j in group_indices:
                    task_j = tasks.loc[idx_j]
                    # Check for overlap
                    latest_start = max(task_i['Inicio'], task_j['Inicio'])
                    earliest_end = min(task_i['Fin'], task_j['Fin'])
                    overlap = (earliest_end - latest_start).total_seconds()
                    if overlap > 0:
                        group_indices.add(idx_i)
                        found_group = True
                        break
                if found_group:
                    break
            if not found_group:
                overlapping_groups.append({idx_i})

        # Adjust durations and costs within each overlapping group
        for group_indices in overlapping_groups:
            group_tasks = tasks.loc[list(group_indices)]

            # Calculate Total h t as the sum of durations
            total_ht = group_tasks['Duracion'].sum()

            # Find the maximum Total ($) among the tasks
            max_total_cost = group_tasks['Total ($)'].max()

            # Calculate overlapping duration (merged intervals within group)
            group_intervals = list(zip(group_tasks['Inicio'], group_tasks['Fin']))
            overlap_hours = merge_intervals(group_intervals)

            # Calculate adjustment factor
            adjustment_factor = overlap_hours / total_ht if total_ht != 0 else 1.0

            # Calculate Total proporcional and Total Proporcional ($) for each task
            for idx in group_indices:
                task = tasks.loc[idx]
                duracion = task['Duracion']
                total_proporcional = duracion / total_ht if total_ht != 0 else 0
                total_proporcional_cost = total_proporcional * max_total_cost
                duracion_ajustada = duracion * adjustment_factor

                # Update the original dataframe
                original_idx = task['index']
                df.loc[original_idx, 'Total h t'] = total_ht
                df.loc[original_idx, 'Total proporcional'] = total_proporcional
                df.loc[original_idx, 'Total Proporcional ($)'] = total_proporcional_cost
                df.loc[original_idx, 'Duracion Ajustada (H)'] = duracion_ajustada

    # For tasks that didn't overlap, set Total Proporcional ($) to Total ($) and Duracion Ajustada to Duracion
    df.loc[df['Total Proporcional ($)'] == 0, 'Total Proporcional ($)'] = df['Total ($)']
    df.loc[df['Duracion Ajustada (H)'] == 0, 'Duracion Ajustada (H)'] = df['Duracion']

    # Drop temporary columns if needed
    df = df.drop(columns=['Inicio', 'Fin', 'Duracion', 'Total ($)', 'Total h t',
                          'Total proporcional', 'Horas Reales', 'Hora diurna', 'Hora nocturna',
                          'Hora domingo'])

    print("\nWarehouse Proportional:\n", df)

    return df


def group_operations(df_delivery, df_warehouse_proporcional, start_date=None, end_date=None):
    # Filter by date if start_date and end_date are provided
    if start_date:
        df_delivery = df_delivery[df_delivery['Fecha'] >= start_date]
        df_warehouse_proporcional = df_warehouse_proporcional[df_warehouse_proporcional['Fecha'] >= start_date]
    if end_date:
        df_delivery = df_delivery[df_delivery['Fecha'] <= end_date]
        df_warehouse_proporcional = df_warehouse_proporcional[df_warehouse_proporcional['Fecha'] <= end_date]

    # ------------------------------------------------------------------------------------------------------------------

    df_warehouse_proporcional['Horas trabajadas'] = (df_warehouse_proporcional['Horas diurnas'] +
                                                     df_warehouse_proporcional['Horas nocturnas'])

    df_warehouse_proporcional['Horas a cobrar'] = np.ceil(df_warehouse_proporcional['Horas trabajadas'])

    # Group by operation (fecha, cliente, bodega/ruta) for warehouse
    grouped_warehouse = df_warehouse_proporcional.groupby(['Fecha', 'Idcontacto', 'Bodega']).agg({
        'Cliente': 'first',
        'Codigo': 'count',  # Number of people involved
        'Horas diurnas': 'max',
        'Horas nocturnas': 'max',
        'Horas trabajadas': 'max',
        'Total Proporcional ($)': 'sum',
        'Horas a cobrar': 'max'
    }).reset_index()

    grouped_warehouse.rename(columns={'Codigo': 'Number of People', 'Horas diurnas': 'Horas diurnas pagadas',
                                      'Horas nocturnas': 'Horas nocturnas pagadas',
                                      'Total': 'Total de horas pagadas',
                                      'Total Proporcional ($)': 'Total Pagado Proporcional ($)'},
                             inplace=True)
    # grouped_warehouse['Horas extra de operacion diurna'] = (grouped_warehouse['Horas diurnas pagadas'] /
    #                                                         grouped_warehouse['Number of People'])
    # grouped_warehouse['Horas extra de operacion nocturna'] = (grouped_warehouse['Horas nocturnas pagadas'] /
    #                                                           grouped_warehouse['Number of People'])
    # grouped_warehouse['Horas extra - total'] = (grouped_warehouse['Total de horas pagadas'] /
    #                                             grouped_warehouse['Number of People'])
    # grouped_warehouse['Horas Pagadas - total'] = (grouped_warehouse['Total de horas pagadas'])

    # Strip whitespace from column names if any
    df_delivery.columns = df_delivery.columns.str.strip()

    # Group by operation (fecha, cliente, ruta) for delivery
    grouped_delivery = df_delivery.groupby(['Fecha', 'Cliente', 'Ruta']).agg({
        'Codigo': 'count',  # Number of people involved
        'Horas diurnas': 'max',
        'Horas nocturnas': 'max',
        'Total': 'max',
        'Total ($)': 'sum',
    }).reset_index()

    grouped_delivery.rename(columns={'Codigo': 'Number of People', 'Horas diurnas': 'Horas diurnas pagadas',
                                     'Horas nocturnas': 'Horas nocturnas pagadas',
                                     'Total': 'Total de horas pagadas', 'Total ($)': 'Total Pagado ($)'}, inplace=True)
    grouped_delivery['Horas extra de operacion diurna'] = (grouped_delivery['Horas diurnas pagadas'] /
                                                           grouped_delivery['Number of People'])
    grouped_delivery['Horas extra de operacion nocturna'] = (grouped_delivery['Horas nocturnas pagadas'] /
                                                             grouped_delivery['Number of People'])
    grouped_delivery['Horas extra - total'] = (grouped_delivery['Total de horas pagadas'] /
                                               grouped_delivery['Number of People'])
    grouped_delivery['Horas Pagadas - total'] = (grouped_delivery['Total de horas pagadas'])

    # Individual records remain unchanged for analysis by person
    df_warehouse_individual = df_warehouse_proporcional.copy()

    df_delivery_individual = df_delivery.copy()

    # # Calculate %Δ (percentage delta) for warehouse
    # grouped_warehouse['%Δ'] = grouped_warehouse.groupby(
    #     'Idcontacto')['Total Pagado Proporcional ($)'].pct_change() * 100

    # Calculate %Δ (percentage delta) for delivery
    grouped_delivery['%Δ'] = grouped_delivery.groupby('Cliente')['Total Pagado ($)'].pct_change() * 100

    df_delivery_individual.rename(columns={'Total': 'Total (H)'}, inplace=True)

    df_warehouse_individual = df_warehouse_individual.drop(columns=['Cargo', 'Horas diurnas ($)',
                                                                    'Horas trabajadas', 'Horas nocturnas ($)'])
    # ----------------------------------------------------------------------------------------------------------------------

    # Convert 'Cliente' to uppercase
    df_warehouse_individual['Cliente'] = df_warehouse_individual['Cliente'].str.upper()

    # Convert 'total' and 'Total ($)' to numeric
    df_warehouse_individual['Duracion Ajustada (H)'] = pd.to_numeric(df_warehouse_individual['Duracion Ajustada (H)'],
                                                                     errors='coerce')
    # man_power_qty['Total ($)'] = pd.to_numeric(man_power_qty['Total ($)'], errors='coerce')
    df_warehouse_individual['Total Proporcional ($)'] = pd.to_numeric(df_warehouse_individual['Total Proporcional ($)'],
                                                                      errors='coerce')

    # Ensure 'Fecha' is in datetime format
    df_warehouse_individual['Fecha'] = pd.to_datetime(df_warehouse_individual['Fecha'], format='%d/%m/%y',
                                                      errors='coerce')

    # Convert 'Hora de inicio' and 'Hora de finalizacion' to strings
    df_warehouse_individual['Hora de inicio'] = df_warehouse_individual['Hora de inicio'].astype(str)
    df_warehouse_individual['Hora de finalizacion'] = df_warehouse_individual['Hora de finalizacion'].astype(str)

    # Combine 'Fecha' and 'Hora de inicio' / 'Hora de finalizacion' using vectorized operations
    df_warehouse_individual['Start DateTime'] = pd.to_datetime(
        df_warehouse_individual['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df_warehouse_individual['Hora de inicio'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )

    df_warehouse_individual['End DateTime'] = pd.to_datetime(
        df_warehouse_individual['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df_warehouse_individual[
            'Hora de finalizacion'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )

    # Calculate duration in hours
    df_warehouse_individual['Duration'] = (
                                                  df_warehouse_individual['End DateTime'] - df_warehouse_individual[
                                              'Start DateTime']
                                          ).dt.total_seconds() / 3600.0

    # Handle cases where duration is negative or missing
    df_warehouse_individual['Duration'] = df_warehouse_individual['Duration'].fillna(0)
    df_warehouse_individual.loc[df_warehouse_individual['Duration'] < 0, 'Duration'] = 0

    # Correctly calculate total operation duration per group
    operation_durations = df_warehouse_individual.groupby(['Fecha', 'Idcontacto', 'Bodega']).agg({
        'Start DateTime': 'min',
        'End DateTime': 'max'
    }).reset_index()

    operation_durations['Total Operation Duration'] = (
                                                              operation_durations['End DateTime'] - operation_durations[
                                                          'Start DateTime']
                                                      ).dt.total_seconds() / 3600.0

    operation_durations['Total Operation Duration'] = np.ceil(operation_durations['Total Operation Duration'])

    # Sum total person-hours and total amount paid to workers per group
    person_hours = df_warehouse_individual.groupby(['Fecha', 'Idcontacto', 'Bodega']).agg({
        'Cliente': 'first',
        'Codigo': 'count',
        'Duracion Ajustada (H)': 'sum',  # Total person-hours worked
        'Total Proporcional ($)': 'sum'  # Total amount paid to workers
    }).reset_index()

    # ----------------------------------------------------------------------------------------------------------------------
    # Overtime paid by employee for deliveries and warehouses

    df_delivery_individual['Year-Month'] = df_delivery_individual['Fecha'].dt.to_period('M')

    # Sum total person-hours and total amount paid to workers per group
    overtime_paid_hours_routes = df_delivery_individual.groupby(['Year-Month', 'Codigo', 'Nombre']).agg(
        {
            'Total (H)': 'sum',  # Total person-hours worked
            'Total ($)': 'sum'  # Total amount paid to workers
        }).reset_index()

    overtime_paid_hours_routes = overtime_paid_hours_routes.sort_values(by=['Year-Month', 'Total ($)'], ascending=False)

    df_warehouse_individual['Year-Month'] = df_warehouse_individual['Fecha'].dt.to_period('M')

    # Sum total person-hours and total amount paid to workers per group
    overtime_paid_hours_warehouse = df_warehouse_individual.groupby(['Year-Month', 'Codigo', 'Nombre']).agg(
        {
            'Duracion Ajustada (H)': 'sum',  # Total person-hours worked
            'Total Proporcional ($)': 'sum'  # Total amount paid to workers
        }).reset_index()

    df_warehouse_individual = df_warehouse_individual.sort_values(by=['Fecha', 'Bodega'], ascending=False)

    overtime_paid_hours_warehouse = overtime_paid_hours_warehouse.sort_values(by=['Year-Month',
                                                                                  'Total Proporcional ($)'],
                                                                              ascending=False)

    overtime_paid_hours_routes.rename(columns={'Total': 'Total (H)'}, inplace=True)

    print("\nProportional Warehouse overtime (grouped by client):\n", grouped_warehouse)
    print("\nProportional Warehouse overtime (individual):\n", df_warehouse_individual)

    print("\nRouting and delivery overtime (grouped by route):\n", grouped_delivery)
    print("\nRouting and delivery overtime (individual):\n", df_delivery_individual)

    print("\nGrouped paid overtime by employee (Deliveries):\n", overtime_paid_hours_routes)
    print("\nGrouped paid overtime by employee (Warehouses):\n", overtime_paid_hours_warehouse)

    df_warehouse_individual = df_warehouse_individual.drop(columns=['Horas a cobrar', 'Is_Sunday',
                                                                    'Duracion Ajustada (H)', 'Start DateTime',
                                                                    'Duration', 'Year-Month', 'End DateTime'])

    return (overtime_paid_hours_routes, overtime_paid_hours_warehouse, operation_durations, person_hours,
            grouped_warehouse, df_warehouse_individual, grouped_delivery, df_delivery_individual)


def income_calculator(grouped_warehouse, income_overtime_client):
    # Drop the specified columns
    df_warehouse_ot_client = grouped_warehouse.copy()

    df_warehouse_ot_client.rename(columns={'Total Pagado Proporcional ($)': 'Total a pagar ($)'}, inplace=True)

    # print("Client price for overtime hours:\n", income_overtime_client)
    # print("Overtime operations by client:\n", df_warehouse_ot_client)

    # Merge with client rates
    man_power_qty_vs_income = pd.merge(
        df_warehouse_ot_client,
        income_overtime_client[['Idcontacto', 'Precio H. Extra']],
        how='left',
        on='Idcontacto'
    )

    # Calculate income from client
    man_power_qty_vs_income['Ingreso Bruto ($)'] = (
            man_power_qty_vs_income['Precio H. Extra'] * man_power_qty_vs_income['Horas a cobrar']
    )

    # Calculate net income
    man_power_qty_vs_income['Ingreso Neto ($)'] = (
            man_power_qty_vs_income['Ingreso Bruto ($)'] - man_power_qty_vs_income['Total a pagar ($)']
    )

    # Set the float format to two decimal places
    pd.set_option('display.float_format', '{:.2f}'.format)

    # Calculate Margin (%)
    man_power_qty_vs_income['Margin (%)'] = np.where(
        man_power_qty_vs_income['Ingreso Bruto ($)'] != 0,
        (man_power_qty_vs_income['Ingreso Neto ($)'] / man_power_qty_vs_income['Ingreso Bruto ($)']) * 100,
        0
    )

    # Round Margin (%) to two decimal places
    man_power_qty_vs_income['Margin (%)'] = man_power_qty_vs_income['Margin (%)'].round(2)

    # Add 'Year-Month' column
    man_power_qty_vs_income['Year-Month'] = man_power_qty_vs_income['Fecha'].dt.to_period('M')

    print("Detailed overtime report by operation:\n", man_power_qty_vs_income)

    # Aggregate by 'Year-Month' and 'Bodega'
    monthly_bodega_aggregation = man_power_qty_vs_income.groupby(['Year-Month', 'Bodega']).agg({
        'Horas a cobrar': 'sum',
        'Horas trabajadas': 'sum',
        'Total a pagar ($)': 'sum',
        'Ingreso Bruto ($)': 'sum',
        'Ingreso Neto ($)': 'sum'
    }).reset_index()

    # Aggregate by 'Year-Month' and 'Bodega'
    monthly_client_aggregation = man_power_qty_vs_income.groupby(['Year-Month', 'Cliente']).agg({
        'Horas a cobrar': 'sum',
        'Horas trabajadas': 'sum',
        'Total a pagar ($)': 'sum',
        'Ingreso Bruto ($)': 'sum',
        'Ingreso Neto ($)': 'sum'
    }).reset_index()

    # Calculate Margin (%) for aggregated data
    monthly_bodega_aggregation['Margin (%)'] = np.where(
        monthly_bodega_aggregation['Ingreso Bruto ($)'] != 0,
        (monthly_bodega_aggregation['Ingreso Neto ($)'] / monthly_bodega_aggregation['Ingreso Bruto ($)']) * 100,
        0
    )

    # Calculate Margin (%) for aggregated data
    monthly_client_aggregation['Margin (%)'] = np.where(
        monthly_client_aggregation['Ingreso Bruto ($)'] != 0,
        (monthly_client_aggregation['Ingreso Neto ($)'] / monthly_client_aggregation['Ingreso Bruto ($)']) * 100,
        0
    )

    man_power_qty_vs_income.drop(columns=['Year-Month'], inplace=True)

    # Sort and reset index for clarity
    monthly_bodega_aggregation = monthly_bodega_aggregation.sort_values(by=['Year-Month', 'Bodega'])
    monthly_bodega_aggregation.reset_index(drop=True, inplace=True)
    # Sort and reset index for clarity
    monthly_client_aggregation_month_client = monthly_client_aggregation.sort_values(by=['Year-Month', 'Cliente'])
    monthly_client_aggregation_month_client.reset_index(drop=True, inplace=True)

    # Display the final DataFrames for 2024
    man_power_qty_vs_income = man_power_qty_vs_income[man_power_qty_vs_income['Fecha'].dt.year == 2024]
    # man_power_qty_vs_income.drop(columns=['Start DateTime', 'End DateTime'], inplace=True)
    monthly_bodega_aggregation['Year'] = monthly_bodega_aggregation['Year-Month'].astype(str).apply(
        lambda x: int(x.split('-')[0]))
    monthly_bodega_aggregation = monthly_bodega_aggregation[monthly_bodega_aggregation['Year'] == 2024]
    monthly_bodega_aggregation.drop(columns=['Year'], inplace=True)
    monthly_client_aggregation_month_client = monthly_client_aggregation_month_client[
        monthly_client_aggregation_month_client['Year-Month'].dt.year == 2024]

    print("\nMonthly man power qty vs income dataframe by Warehouse:\n", monthly_bodega_aggregation)
    print("\nMonthly man power qty vs income dataframe by Client:\n", monthly_client_aggregation_month_client)
    # print("\nMonthly man power qty vs income comparison:\n", man_power_qty_vs_income)

    return (monthly_client_aggregation_month_client, monthly_bodega_aggregation, monthly_bodega_aggregation,
            man_power_qty_vs_income)


def main():
    pd.set_option(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.expand_frame_repr", False
    )

    print("Main: Prompting user to input data...")

    # Prompt the user for the start and end dates
    date_format = ["%d-%m-%Y", "%d-%m-%y"]

    start_date = None
    end_date = None

    # Get start date
    while start_date is None:
        user_input = input("Enter the start date (dd-mm-yy or dd-mm-yyyy): ")
        for fmt in date_format:
            try:
                start_date = datetime.strptime(user_input, fmt)
                break
            except ValueError:
                continue
        if start_date is None:
            print("Invalid date format. Please try again.")

    # Get end date
    while end_date is None:
        user_input = input("Enter the end date (dd-mm-yy or dd-mm-yyyy): ")
        for fmt in date_format:
            try:
                end_date = datetime.strptime(user_input, fmt)
                break
            except ValueError:
                continue
        if end_date is None:
            print("Invalid date format. Please try again.")

    # Convert dates to strings for further processing
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    # Convert start_date and end_date to datetime for filtering
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    print("Main: Loading data...")

    df_delivery, df_warehouse, df_salary, income_overtime_client = load_data(start_date, end_date)

    df_delivery, df_warehouse, df_salary, income_overtime_client = (
        data_normalization(df_warehouse, df_delivery, df_salary, income_overtime_client))

    # Calculate costs and profitability
    df_delivery, df_warehouse = cost_calculator(df_delivery, df_warehouse, df_salary, start_date, end_date)

    # Apply the function to your dataframe
    df_warehouse_proportional = adjust_overlapping_costs(df_warehouse)

    (overtime_paid_hours_routes, overtime_paid_hours_warehouse, operation_durations, person_hours,
     grouped_warehouse, df_warehouse_individual, grouped_delivery, df_delivery_individual) = (
        group_operations(df_delivery, df_warehouse, start_date, end_date))

    monthly_client_aggregation_month_client, monthly_bodega_aggregation, monthly_bodega_aggregation, \
        man_power_qty_vs_income = (income_calculator(grouped_warehouse, income_overtime_client))


if __name__ == "__main__":
    main()
