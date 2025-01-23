from rich.progress import Progress
import time
import pandas as pd
import os
from utils import get_base_output_path

def data_screening(saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos, rpshd_despachos,
                   wl_ingresos, inmovih_table, dispatched_inventory):

    # INGRESOS ---------------------------------------------------------------------------------------------------------

    # Función para asignar bodega de acuerdo al idubica
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Screening Data: ", total=14)

        def asignar_ubicacion(idubica):

            if idubica == 'P00000':
                return 'PISO'
            elif idubica.startswith('E'):
                return 'BODE'
            elif idubica in ['PE0000']:
                return 'BODE'
            elif idubica in ['C2PD', 'C2PE', 'C2PF', 'C2PG', 'C2PL', 'C1PA', 'C2PN',
                             'C2PA', 'C2P0', 'C2PK', 'C2PJ', 'C2PI']:
                return 'INTEMPERIE'
            elif idubica.startswith('A'):
                return 'BODA'
            elif idubica in ['PA0000']:
                return 'BODA'
            elif idubica.startswith('C'):
                return 'BODC'
            elif idubica in ['PC0000']:
                return 'BODC'
            elif idubica.startswith('G'):
                return 'BODG'
            elif idubica in ['PG0000']:
                return 'BODG'
            elif idubica in [
                'C1PA', 'C2PJ', 'C2PA', 'C2P0', 'C2PN', 'C2PO', 'C1PE', 'C1PF',
                'C1PG', 'C1PL', 'C1PJ', 'C1PK', 'C1PI', 'C2PJ', 'C2PK', 'C2PI'
            ]:
                return 'BODJ'
            elif idubica in ['PN0000']:
                return 'BODJ'
            elif idubica.startswith(('P', 'B', 'M', 'V')):
                return 'BODJ'
            else:
                return 'DESCONOCIDO'

        # Step: Defining object location
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Apply the location assignment logic to `saldo_inventory`
        saldo_inventory['bodega'] = saldo_inventory['idubica'].apply(asignar_ubicacion)

        # Step: Locating objects
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Asignar y filtrar registro_ingresos... key = idingreso -------------------------------------------------------

        # print("registro ingresos:\n", registro_ingresos)
        registro_ingresos['idingreso'] = registro_ingresos['idingreso'].astype(str)
        saldo_inventory['idingreso'] = saldo_inventory['idingreso'].astype(str)
        saldo_inventory['bodega'] = saldo_inventory['bodega'].astype(str)

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        registro_ingresos = registro_ingresos.groupby('idingreso').agg({
            'fecha': 'first',
            'items': 'first',
            'transtatus': 'first',
            'descrip': 'first',
            'idcontacto': 'first',
            'retnum': 'first',
            # 'bodega_x': 'first',
        }).reset_index()

        # Step: Grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        registro_ingresos = pd.merge(registro_ingresos, saldo_inventory[['idingreso', 'idubica', 'bodega']],
                                     on='idingreso', how='left')

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Tabla registro_salidas - Asignación de bodegas

        # Crear la nueva columna 'bodega' en rpsdt_productos usando la función asignar_ubicación
        rpsdt_productos['bodega'] = rpsdt_productos['idubica'].apply(asignar_ubicacion)

        # Step: Defining object location
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Agrupar 'rpsdt_productos' y obtener el primer valor de 'bodega' e 'idubica' por 'idingreso'
        rpsdt_productos_agrupado_ingreso = rpsdt_productos.groupby('idingreso').agg({
            'bodega': 'first',
            'idubica': 'first'
        }).reset_index()

        # Step: Grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Convertir ambas columnas a str
        registro_salidas['idingreso'] = registro_salidas['idingreso'].astype('str')
        rpsdt_productos_agrupado_ingreso['idingreso'] = rpsdt_productos_agrupado_ingreso['idingreso'].astype('str')

        # Step: Preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Hacer el merge con 'registro_salidas'
        registro_salidas = pd.merge(registro_salidas, rpsdt_productos_agrupado_ingreso, on='idingreso', how='left')

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # # Guardar resultados en excel
        # output_path = r'C:\Users\josemaria\Downloads\registro_salidas_post_merge.csv'
        # registro_salidas.to_csv(output_path, index=True)

        # Aplicar lambda para asingar DESCONOCIDO a idubica
        registro_salidas['idubica'] = registro_salidas['idubica'].apply(
            lambda x: "DESCONOCIDO" if pd.isna(x) or str(x).strip() == "" else x)
        registro_salidas['bodega'] = registro_salidas['bodega'].apply(
            lambda x: "DESCONOCIDO" if pd.isna(x) or str(x).strip() == "" else x)
        # Aplicar lambda para asingar DESCONOCIDO a idubica
        registro_ingresos['idubica'] = registro_ingresos['idubica'].apply(
            lambda x: "DESCONOCIDO" if pd.isna(x) or str(x).strip() == "" else x)
        registro_ingresos['bodega'] = registro_ingresos['bodega'].apply(
            lambda x: "DESCONOCIDO" if pd.isna(x) or str(x).strip() == "" else x)

        # Step: Defining unknown locations
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Renaming columns
        registro_salidas.rename(columns={'bodega_x': 'bodega', 'idubica_x': 'idubica'}, inplace=True)

        inmovih_table.rename(columns={'bodega_x': 'bodega', 'idubica_x': 'idubica'}, inplace=True)

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Paso 3: Agrupar 'registro_salidas' por 'trannum' y obtener la primera 'bodega' e 'idubica'
        registro_salidas_agrupado = registro_salidas.groupby('trannum').agg({
            'bodega': 'first',
            'idubica': 'first'
        }).reset_index()

        # Step: Grouping data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Merge con 'inmovih_table'
        inmovih_table = pd.merge(inmovih_table, registro_salidas_agrupado, on='trannum', how='left')

        # Step: Merging data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Paso 5: Eliminar filas donde 'bodega' sea NaN en cada DataFrame
        saldo_inventory = saldo_inventory.dropna(subset=['bodega'])
        registro_ingresos = registro_ingresos.dropna(subset=['bodega'])
        rpsdt_productos = rpsdt_productos.dropna(subset=['bodega'])
        inmovih_table = inmovih_table.dropna(subset=['bodega'])
        registro_salidas = registro_salidas.dropna(subset=['bodega'])

        saldo_inventory['idcontacto'] = saldo_inventory['idcontacto'].astype(str).fillna('')
        registro_ingresos['idcontacto'] = registro_ingresos['idcontacto'].astype(str).fillna('')
        wl_ingresos['idcontacto'] = wl_ingresos['idcontacto'].astype(str).fillna('')
        rpsdt_productos['idcontacto'] = rpsdt_productos['idcontacto'].astype(str).fillna('')
        registro_salidas['idcontacto'] = registro_salidas['idcontacto'].astype(str).fillna('')
        inmovih_table['idcontacto'] = inmovih_table['idcontacto'].astype(str).fillna('')

        # Final Step: Updating `bodega` based on `idcontacto`
        def update_bodega_based_on_idcontacto(row):
            idcontacto = str(row['idcontacto']) if pd.notna(row['idcontacto']) else ''

            if row['idcontacto'].endswith('_c') and row['bodega'] != 'BODC':
                return 'BODC'
            elif row['idcontacto'].endswith('_e') and row['bodega'] != 'BODE':
                return 'BODE'
            elif row['idcontacto'].endswith('_opl') and row['bodega'] != 'OPL':
                return 'OPL'
            else:
                return row['bodega']

        # Apply the logic to update `bodega` for all relevant DataFrames
        for df in [saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos]:
            df['bodega'] = df.apply(update_bodega_based_on_idcontacto, axis=1)

        # Step: Cleaning data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("\nData Screening completed successfully.\n")

    output_path = os.path.join(get_base_output_path(), 'registro_ingresos_data_screening.csv')
    registro_ingresos.to_csv(output_path, index=False)


    return (saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos, rpshd_despachos, wl_ingresos,
            inmovih_table, dispatched_inventory)
