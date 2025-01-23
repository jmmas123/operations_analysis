from rich.progress import Progress
import os
import pandas as pd
import socket
import time


def inventory_proportions_by_product(saldo_inventory, supplier_info):
    with Progress() as progress:
        # Add a task with a total number of steps
        task = progress.add_task("[green]Clustering Clients inventory data: ", total=14)
        # Leer los datos especificando los tipos de datos
        if os.name == 'nt':  # Windows
            # Network path for Windows
            inmodelo_clasificacion = pd.read_excel(
                '\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\varios\modelos_clasificacion.xlsx')
        else:  # macOS or others
            hostname = socket.gethostname()
            if 'JM-MS.local' in hostname:  # For Mac Studio
                inmodelo_clasificacion = pd.read_excel(
                    r'/Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')
            elif 'MacBook-Pro.local' in hostname:  # For MacBook Pro
                inmodelo_clasificacion = pd.read_excel(
                    r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')
            else:
                raise ValueError(f"Unknown hostname: {hostname}. Unable to determine file path.")

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory = saldo_inventory[saldo_inventory['idstatus'] == '01']

        saldo_inventory['fecha'] = pd.to_datetime(saldo_inventory['fecha'])
        # Ordenar fechas de más reciente a más antiguas
        saldo_inventory = saldo_inventory.sort_values(by='fecha', ascending=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Asegurar que la columna 'idubica' y 'idmodelo' sea de tipo string
        saldo_inventory['idubica'].astype(str)
        saldo_inventory['idmodelo'].astype(str)
        saldo_inventory['inicial'] = pd.to_numeric(saldo_inventory['inicial'], errors='coerce')
        saldo_inventory['salidas'] = pd.to_numeric(saldo_inventory['salidas'], errors='coerce')
        saldo_inventory['pesokgs'] = pd.to_numeric(saldo_inventory['pesokgs'], errors='coerce')
        inmodelo_clasificacion['idmodelo'].astype(str)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Eliminar espacios en blanco de los valores
        saldo_inventory['idmodelo'].str.strip()
        inmodelo_clasificacion['idmodelo'].str.strip()

        # Asegurar que las columnas sean de tipo string y eliminar espacios en blanco
        saldo_inventory['idmodelo'] = saldo_inventory['idmodelo'].astype(str).str.strip()
        inmodelo_clasificacion['idmodelo'] = inmodelo_clasificacion['idmodelo'].astype(str).str.strip()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory = pd.merge(saldo_inventory, inmodelo_clasificacion, how='left', on='idmodelo')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Update 'inicial' with values from 'cubicaje' where 'cubicaje' is not NaN
        saldo_inventory.loc[saldo_inventory['cubicaje'].notna(), 'inicial'] = saldo_inventory['cubicaje']

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Drop the specified columns
        saldo_inventory = saldo_inventory.drop(columns=['descrip', 'clasificacion', 'cubicaje'])

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Determine the maximum length of 'idcontacto' values in both DataFrames
        max_length = max(saldo_inventory['idcontacto'].str.len().max(),
                         supplier_info['idcontacto'].str.len().max())

        # Pad 'idcontacto' values with leading zeros to match the maximum length
        saldo_inventory.loc[:, 'idcontacto'] = saldo_inventory['idcontacto'].str.zfill(max_length)
        supplier_info['idcontacto'] = supplier_info['idcontacto'].str.zfill(max_length)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory['dup_key'] = (saldo_inventory['idingreso'] +
                                      saldo_inventory['itemno'])

        saldo_inventory = saldo_inventory.drop_duplicates(subset='dup_key', keep='first')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory_grouped = saldo_inventory.groupby([
            'idmodelo'
        ]).agg({'idcoldis': 'first', 'inicial': 'sum',
                'pesokgs': 'sum', 'itemno': 'count'}).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns
        saldo_inventory_grouped.rename(columns={
            'pesokgs': 'Units',
            'inicial': 'CBM',
            'idmodelo': 'ProductID',
            'idcoldis': 'ModelID',
            'itemno': 'Pallets',
        }, inplace=True)

        saldo_inventory_grouped['CBM'] = pd.to_numeric(saldo_inventory_grouped['CBM'], errors='coerce')
        saldo_inventory_grouped['Pallets'] = pd.to_numeric(saldo_inventory_grouped['Pallets'], errors='coerce')
        saldo_inventory_grouped['Units'] = pd.to_numeric(saldo_inventory_grouped['Units'], errors='coerce')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate the total CBM, pallets, and units for the whole inventory
        total_cbm = saldo_inventory_grouped['CBM'].sum()
        total_pallets = saldo_inventory_grouped['Pallets'].sum()
        total_units = saldo_inventory_grouped['Units'].sum()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        total_cbm = pd.to_numeric(total_cbm, errors='coerce')
        total_pallets = pd.to_numeric(total_pallets, errors='coerce')
        total_units = pd.to_numeric(total_units, errors='coerce')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Add percentage columns
        saldo_inventory_grouped['CBM %'] = (saldo_inventory_grouped['CBM'] / total_cbm) * 100
        saldo_inventory_grouped['Pallets %'] = (saldo_inventory_grouped['Pallets'] / total_pallets) * 100
        saldo_inventory_grouped['units %'] = (saldo_inventory_grouped['Units'] / total_units) * 100

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory_grouped = saldo_inventory_grouped.sort_values(by='CBM %', ascending=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)
    print("\nActual Client inventory proportions to date:\n", saldo_inventory_grouped)
    print("\nClustering process complete.\n")
