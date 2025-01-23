from rich.progress import Progress
import os
from utils import get_base_output_path
import pandas as pd
import time
import numpy as np
from data_processing import resolve_bodega


def monthly_receptions_summary(registro_ingresos, supplier_info, inventario_sin_filtro, rpsdt_productos):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Analysing historic reception Data: ", total=19)

        registro_ingresos['fecha'] = pd.to_datetime(registro_ingresos['fecha'], errors='coerce')
        registro_ingresos['items'] = pd.to_numeric(registro_ingresos['items'], errors='coerce')
        registro_ingresos['idcontacto'] = registro_ingresos['idcontacto'].astype(str)
        registro_ingresos['idingreso'] = registro_ingresos['idingreso'].astype(str)

        inventario_sin_filtro['fecha'] = pd.to_datetime(inventario_sin_filtro['fecha'], errors='coerce')
        inventario_sin_filtro['modifica'] = pd.to_datetime(inventario_sin_filtro['modifica'], errors='coerce')
        inventario_sin_filtro['ingresa'] = pd.to_datetime(inventario_sin_filtro['ingresa'], errors='coerce')
        inventario_sin_filtro['inicial'] = pd.to_numeric(inventario_sin_filtro['inicial'], errors='coerce')
        inventario_sin_filtro['salidas'] = pd.to_numeric(inventario_sin_filtro['salidas'], errors='coerce')
        inventario_sin_filtro['idpedido'] = pd.to_numeric(inventario_sin_filtro['idpedido'], errors='coerce')
        inventario_sin_filtro['pesokgs'] = pd.to_numeric(inventario_sin_filtro['pesokgs'], errors='coerce')
        inventario_sin_filtro['idcontacto'] = inventario_sin_filtro['idcontacto'].astype(str)
        inventario_sin_filtro['idingreso'] = inventario_sin_filtro['idingreso'].astype(str)

        output_path = os.path.join(get_base_output_path(), 'registro_ingresos_monthly_summary.csv')
        registro_ingresos.to_csv(output_path, index=True)
        output_path = os.path.join(get_base_output_path(), 'inventario_sin_filtro_montly_summary.csv')
        inventario_sin_filtro.to_csv(output_path, index=True)

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        monthly_registro_ingresos = registro_ingresos
        monthly_inventario_sin_filtro = inventario_sin_filtro

        # Check if 'modifica' month is greater than 'fecha' month and 'inicial' is 0
        monthly_inventario_sin_filtro.loc[:, 'ddma'] = np.where(
            (monthly_inventario_sin_filtro['modifica'].dt.month >= monthly_inventario_sin_filtro['fecha'].dt.month) &
            (monthly_inventario_sin_filtro['inicial'] == 0) &
            (monthly_inventario_sin_filtro['salidas'] == 0) &
            (monthly_inventario_sin_filtro['pesokgs'] == 0),
            monthly_inventario_sin_filtro['idpedido'],  # Assign 'idpedido' if both conditions are True
            np.nan  # Otherwise, assign NaN
        )

        # Step: Defining partial product shipment
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Casting
        monthly_inventario_sin_filtro.loc[:, 'ddma'] = pd.to_numeric(monthly_inventario_sin_filtro['ddma'],
                                                                     errors='coerce')
        monthly_registro_ingresos.loc[:, 'idcontacto'] = monthly_registro_ingresos['idcontacto'].astype(str).str.strip()
        monthly_inventario_sin_filtro.loc[:, 'idcontacto'] = monthly_inventario_sin_filtro['idcontacto'].astype(
            str).str.strip()
        supplier_info['idcontacto'] = supplier_info['idcontacto'].astype(str).str.strip()

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Determine the maximum length of 'idcontacto' values in both DataFrames
        max_length = max(monthly_registro_ingresos['idcontacto'].str.len().max(),
                         supplier_info['idcontacto'].str.len().max())

        # Pad 'idcontacto' values with leading zeros to match the maximum length
        monthly_registro_ingresos.loc[:, 'idcontacto'] = monthly_registro_ingresos['idcontacto'].str.zfill(max_length)
        monthly_inventario_sin_filtro.loc[:, 'idcontacto'] = monthly_inventario_sin_filtro['idcontacto'].str.zfill(
            max_length)
        supplier_info['idcontacto'] = supplier_info['idcontacto'].str.zfill(max_length)

        # Step: Preparing keys
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Ordenar las filas filtradas de más recientes a más antiguas
        monthly_registro_ingresos = monthly_registro_ingresos.sort_values(by='fecha', ascending=False)
        monthly_inventario_sin_filtro = monthly_inventario_sin_filtro.sort_values(by='fecha', ascending=False)

        # Step: Sorting values
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Merge the DataFrames
        merged_ingresos_inventario = pd.merge(monthly_registro_ingresos, monthly_inventario_sin_filtro, on='idingreso',
                                              how='left')

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Create dup_key
        merged_ingresos_inventario['dup_key'] = (merged_ingresos_inventario['idingreso'] +
                                                 merged_ingresos_inventario['itemno'])

        # Drop duplicates based on 'idingreso'
        merged_ingresos_inventario = merged_ingresos_inventario.drop_duplicates(subset='dup_key', keep='first')

        # Step: Droping duplicated data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        output_path = os.path.join(get_base_output_path(), 'merged_ingresos_inventario_before_mask.csv')
        merged_ingresos_inventario.to_csv(output_path, index=True)

        # Step: Replacing unknown data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        merged_ingresos_inventario = merged_ingresos_inventario[[
            'idingreso', 'itemno', 'fecha_x', 'descrip', 'idcontacto_x', 'bodega', 'idubica_y', 'idubica_x', 'idmodelo',
            'idcoldis', 'pesokgs', 'inicial',
            'salidas', 'ddma', 'retnum_x', 'modifica']]

        merged_ingresos_inventario['idcontacto_x'] = merged_ingresos_inventario.rename(
            columns={'idcontacto_x': 'idcontacto'}, inplace=True)

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        resumen_mensual_ingresos_sd = pd.merge(
            merged_ingresos_inventario, rpsdt_productos[['bodega', 'idubica', 'idingreso']], on='idingreso', how='left')

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        resumen_mensual_ingresos_sd['dup_key'] = (resumen_mensual_ingresos_sd['idingreso'] +
                                                  resumen_mensual_ingresos_sd['itemno'])

        resumen_mensual_ingresos_sd = resumen_mensual_ingresos_sd.drop_duplicates(subset='dup_key', keep='first')

        # Step: Droping duplicates
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        resumen_mensual_ingresos_sd['bodega_x'] = resumen_mensual_ingresos_sd['bodega_x'].str.strip().str.upper()
        resumen_mensual_ingresos_sd['bodega_y'] = resumen_mensual_ingresos_sd['bodega_y'].str.strip().str.upper()

        # Apply the function to your dataframe to create a unified Bodega column
        resumen_mensual_ingresos_sd['Bodega'] = resumen_mensual_ingresos_sd.apply(resolve_bodega, axis=1)

        resumen_mensual_ingresos_fact = resumen_mensual_ingresos_sd

        resumen_mensual_ingresos_sd['ddma'] = pd.to_numeric(resumen_mensual_ingresos_sd['ddma'], errors='coerce')
        resumen_mensual_ingresos_sd['inicial'] = pd.to_numeric(resumen_mensual_ingresos_sd['inicial'], errors='coerce')
        resumen_mensual_ingresos_sd['pesokgs'] = pd.to_numeric(resumen_mensual_ingresos_sd['pesokgs'], errors='coerce')

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        resumen_mensual_ingresos_sd['Bodega'] = resumen_mensual_ingresos_sd['Bodega'].fillna("INCOHERENT VALUES")

        # Ensure 'fecha_x' is in datetime format
        resumen_mensual_ingresos_sd['fecha_x'] = pd.to_datetime(resumen_mensual_ingresos_sd['fecha_x'], errors='coerce')

        # Step: Cleaning data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Extract the month and year as a period (e.g., '2023-01')
        resumen_mensual_ingresos_sd['month'] = resumen_mensual_ingresos_sd['fecha_x'].dt.to_period('M')

        # Continue with your aggregation, now grouping by 'month' as well
        resumen_mensual_ingresos = resumen_mensual_ingresos_sd.groupby(['month', 'idcontacto', 'Bodega']).agg({
            'fecha_x': 'first',
            'retnum_x': 'count',
            'pesokgs': 'sum',
            'inicial': 'sum',
            'ddma': 'sum'
        }).reset_index()

        # Step: Grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns accordingly
        resumen_mensual_ingresos.rename(columns={
            # 'month': 'first',
            'pesokgs': 'Unidades',
            'inicial': 'CBM',
            'retnum_x': 'Pallets',
            'ddma': 'Desprendimientos despues del mes de analisis'
        }, inplace=True)

        # Step: Renaming columns
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Adjust 'CBM' by adding 'Desprendimientos despues del mes de analisis' where 'ddma' is not zero
        resumen_mensual_ingresos['CBM'] += resumen_mensual_ingresos['Desprendimientos despues del mes de analisis']

        # Step: Complementing CBM data with partial shipments
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Merge dataframe with incontac to obtain client name
        resumen_mensual_ingresos_clientes = pd.merge(resumen_mensual_ingresos, supplier_info, on='idcontacto',
                                                     how='left')

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        resumen_mensual_ingresos_clientes['descrip'] = resumen_mensual_ingresos_clientes.rename(
            columns={'descrip': 'Cliente'}, inplace=True)

        # print("Ingresos mensuales por idcliente y Bodega (agrupado 1):\n", resumen_mensual_ingresos_clientes)

        # Continue with your aggregation, now grouping by 'month' as well
        resumen_mensual_ingresos_clientes = resumen_mensual_ingresos_clientes.groupby(
            ['month', 'Bodega', 'Cliente']).agg({
            'fecha_x': 'first',
            'idcontacto': 'first',  # Assuming 'idcontacto' is the same within each group
            'Pallets': 'sum',
            'Unidades': 'sum',
            'CBM': 'sum'
        }).reset_index()

        if 'Bodega' in resumen_mensual_ingresos_clientes.columns:
            # Check if column values in 'Bodega' start with 'B'
            if resumen_mensual_ingresos_clientes['Bodega'].astype(str).str.startswith('B').any():
                resumen_mensual_ingresos_clientes.rename(columns={'Bodega': 'bodega'}, inplace=True)

        # Step: Grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        output_path = os.path.join(get_base_output_path(), 'resumen_mensual_ingresos_fact.csv')
        resumen_mensual_ingresos_fact.to_csv(output_path, index=True)
        output_path = os.path.join(get_base_output_path(), 'resumen_mensual_ingresos_sd.csv')
        resumen_mensual_ingresos_sd.to_csv(output_path, index=True)

        # Step: Printing CSV data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("\nHistoric inflow of CBM, pallets and units by client and warehouse:\n", resumen_mensual_ingresos_clientes)
    print("\nMonthly reception data processed correctly.\n")

    return resumen_mensual_ingresos_clientes, resumen_mensual_ingresos_sd, resumen_mensual_ingresos_fact


def monthly_dispatch_summary(registro_salidas, dispatched_inventory, supplier_info):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Analyzing historic dispatch data: ", total=11)

        # Data type conversions
        registro_salidas['fecha'] = pd.to_datetime(registro_salidas['fecha'], errors='coerce')
        registro_salidas['cantidad'] = pd.to_numeric(registro_salidas['cantidad'], errors='coerce')
        registro_salidas['idcontacto'] = registro_salidas['idcontacto'].astype(str)
        registro_salidas['idingreso'] = registro_salidas['idingreso'].astype(str)

        dispatched_inventory['fecha'] = pd.to_datetime(dispatched_inventory['fecha'], errors='coerce')
        dispatched_inventory['ingresa'] = pd.to_datetime(dispatched_inventory['ingresa'], errors='coerce')
        dispatched_inventory['inicial'] = pd.to_numeric(dispatched_inventory['inicial'], errors='coerce')
        dispatched_inventory['salidas'] = pd.to_numeric(dispatched_inventory['salidas'], errors='coerce')
        dispatched_inventory['pesokgs'] = pd.to_numeric(dispatched_inventory['pesokgs'], errors='coerce')
        dispatched_inventory['idcontacto'] = dispatched_inventory['idcontacto'].astype(str)
        dispatched_inventory['idingreso'] = dispatched_inventory['idingreso'].astype(str)

        supplier_info['idcontacto'] = supplier_info['idcontacto'].astype(str).str.strip()

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Pad 'idcontacto' and 'idingreso' to match maximum length
        max_length_idc = max(registro_salidas['idcontacto'].str.len().max(),
                             supplier_info['idcontacto'].str.len().max())
        registro_salidas['idcontacto'] = registro_salidas['idcontacto'].str.zfill(max_length_idc)
        dispatched_inventory['idcontacto'] = dispatched_inventory['idcontacto'].str.zfill(max_length_idc)
        supplier_info['idcontacto'] = supplier_info['idcontacto'].str.zfill(max_length_idc)

        max_length_idi = max(registro_salidas['idingreso'].str.len().max(),
                             dispatched_inventory['idingreso'].str.len().max())
        registro_salidas['idingreso'] = registro_salidas['idingreso'].str.zfill(max_length_idi)
        dispatched_inventory['idingreso'] = dispatched_inventory['idingreso'].str.zfill(max_length_idi)

        # Step: Normalizing keys
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Sort dataframes
        registro_salidas = registro_salidas.sort_values(by='fecha', ascending=False)
        dispatched_inventory = dispatched_inventory.sort_values(by='fecha', ascending=False)

        # Step: Sorting data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Perform a left merge to keep all rows from registro_salidas
        merged_despachos_inventario = pd.merge(
            registro_salidas,
            dispatched_inventory,
            on='idingreso',
            how='left',
            suffixes=('_x', '_y')
        )

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Ensure 'fecha_x' is in datetime format
        merged_despachos_inventario['fecha_x'] = pd.to_datetime(merged_despachos_inventario['fecha_x'], errors='coerce')

        # Extract the month and year as a period
        merged_despachos_inventario['month'] = merged_despachos_inventario['fecha_x'].dt.to_period('M')

        # Step: preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Create a unique key for duplicates
        merged_despachos_inventario['dup_key'] = (merged_despachos_inventario['idingreso'] +
                                                  merged_despachos_inventario['itemno_x'])

        # Drop duplicates based on 'dup_key'
        merged_despachos_inventario = merged_despachos_inventario.drop_duplicates(subset='dup_key', keep='first')

        # Step: Building key and dropping duplicates
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Handle 'DESCONOCIDO' in 'bodega'
        filtered_bodegas = merged_despachos_inventario[merged_despachos_inventario['bodega'] != 'DESCONOCIDO']
        replacement_bodega = filtered_bodegas.groupby('idcontacto_x').filter(lambda x: len(x['bodega'].unique()) == 1)
        replacement_bodega = replacement_bodega.groupby('idcontacto_x')['bodega'].first()
        mask = (merged_despachos_inventario['bodega'] == 'DESCONOCIDO') & merged_despachos_inventario[
            'idcontacto_x'].isin(
            replacement_bodega.index)
        merged_despachos_inventario.loc[mask, 'bodega'] = merged_despachos_inventario.loc[mask, 'idcontacto_x'].map(
            replacement_bodega)

        # Step: Identifying unknowns and cleaning data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        resumen_despachos_cliente_fact = merged_despachos_inventario

        # Continue with your aggregation, now grouping by 'month' as well
        resumen_mensual_despachos = merged_despachos_inventario.groupby(['month', 'idcontacto_x', 'bodega']).agg({
            'fecha_x': 'first',
            'numero': 'count',
            'pesokgs': 'sum',
            'cantidad': 'sum',
        }).reset_index()

        # Step: Grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns
        resumen_mensual_despachos.rename(columns={
            'idcontacto_x': 'idcontacto',
            'bodega': 'Bodega',
            'pesokgs': 'Unidades',
            'cantidad': 'CBM',
            'numero': 'Pallets',
        }, inplace=True)

        # Merge with 'supplier_info' to get 'Cliente' information
        resumen_mensual_despachos_clientes = pd.merge(
            resumen_mensual_despachos, supplier_info[['idcontacto', 'descrip']], on='idcontacto', how='left'
        )

        # Step: Renaming columns and merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename 'descrip' to 'Cliente'
        resumen_mensual_despachos_clientes.rename(columns={'descrip': 'Cliente'}, inplace=True)

        # Group by 'month', 'Bodega', and 'Cliente', summing numerical values
        resumen_mensual_despachos_clientes_grouped = resumen_mensual_despachos_clientes.groupby(
            ['month', 'Bodega', 'Cliente']
        ).agg({
            'fecha_x': 'first',
            'idcontacto': 'first',
            'Pallets': 'sum',
            'Unidades': 'sum',
            'CBM': 'sum',
        }).reset_index()

        # Step: Renaming columns and grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Save the final DataFrame to CSV
        output_path = os.path.join(get_base_output_path(), 'despachos_cliente_bodega_mensual_historico.csv')
        resumen_mensual_despachos_clientes_grouped.to_csv(output_path, index=False)

        # Step: Printing CSV data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    # Print the final DataFrame
    print("\nHistoric outflow of CBM, pallets and units by client and warehouse:\n",
          resumen_mensual_despachos_clientes_grouped)
    print("\nMonthly shipment data processed correctly.\n")

    return resumen_mensual_despachos_clientes_grouped, merged_despachos_inventario, resumen_despachos_cliente_fact
