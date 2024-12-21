import os
import random
import warnings
from datetime import datetime
import pandas as pd
import numpy as np
from rich.progress import Progress
import time

# Suppress all SettingWithCopyWarnings and FutureWarnings
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings('ignore', category=FutureWarning)


# secondary functions --------------------------------------------------------------------------------------------------


def parse_date(date_str):
    for fmt in ('%d-%m-%Y', '%d-%m-%y', '%d/%m/%Y', '%d/%m/%y'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError("Invalid date format. Please enter dates in dd/mm/yy or dd-mm-yy format.")


def filter_dataframes_by_idcontacto(dataframes, idcontacto):
    filtered_dataframes = []
    for df in dataframes:

        # Identify all columns that could represent 'idcontacto'
        idcontacto_columns = [col for col in df.columns if 'idcontacto' in col]
        if idcontacto_columns:

            # Create a mask for rows where any 'idcontacto' column matches the target idcontacto
            mask = pd.Series(False, index=df.index)
            for col in idcontacto_columns:
                mask |= (df[col].astype(str).str.strip() == idcontacto)
            df_filtered = df[mask].copy()
            filtered_dataframes.append(df_filtered)
        else:

            # If 'idcontacto' not in columns, keep the DataFrame as is
            filtered_dataframes.append(df)
    return filtered_dataframes


def filter_dataframes_by_warehouse(dataframes, warehouse):
    filtered_dataframes = []
    for df in dataframes:

        # Identify all columns that could represent 'bodega'
        bodega_columns = [col for col in df.columns if 'bodega' in col]
        if bodega_columns:

            # Create a mask for rows where any 'bodega' column matches the target warehouse
            mask = pd.Series(False, index=df.index)
            for col in bodega_columns:
                mask |= (df[col].astype(str).str.strip() == warehouse)
            df_filtered = df[mask].copy()
            filtered_dataframes.append(df_filtered)
        else:

            # If 'bodega' not in columns, keep the DataFrame as is
            filtered_dataframes.append(df)
    return filtered_dataframes


# Function to resolve Bodega conflicts
def resolve_bodega(row):
    x = row['bodega_x'].strip().upper() if isinstance(row['bodega_x'], str) else row['bodega_x']
    y = row['bodega_y'].strip().upper() if isinstance(row['bodega_y'], str) else row['bodega_y']
    id_x = row['idubica_x'].strip().upper() if isinstance(row['idubica_x'], str) else row['idubica_x']
    id_y = row['idubica'].strip().upper() if isinstance(row['idubica'], str) else row['idubica']

    # # Check for 'TIENDA' and real ID
    # if id_x == 'TIENDA' and pd.notna(id_y) and id_y != 'TIENDA':
    #     return y
    # if id_y == 'TIENDA' and pd.notna(id_x) and id_x != 'TIENDA':
    #     return x

    # Check for one 'DESCONOCIDO' and the other not, with real ID consideration
    if x == "DESCONOCIDO" and y != "DESCONOCIDO":
        return y if (pd.notna(id_y) or id_y != "") and (
                id_y != 'TIENDA' or id_y != "DESCONOCIDO") else "INCOHERENT VALUES"
    if y == "DESCONOCIDO" and x != "DESCONOCIDO":
        return x if (pd.notna(id_x) or id_x != "") and (
                id_x != 'TIENDA' or id_x != "DESCONOCIDO") else "INCOHERENT VALUES"
    if (pd.notna(x) or x == "") and (pd.notna(y) or y == "") and x != y:
        return y if pd.notna(id_y) and id_y != 'TIENDA' or id_y != "DESCONOCIDO" else "INCOHERENT VALUES"
    if (pd.notna(x) or x == "") and (pd.notna(y) or y == "") and x != y:
        return x if pd.notna(id_x) and id_x != 'TIENDA' or id_x != "DESCONOCIDO" else "INCOHERENT VALUES"
    if x == "PISO" and y != "DESCONOCIDO":
        return y if pd.notna(id_y) else "INCOHERENT VALUES"
    if y == "PISO" and x != "DESCONOCIDO":
        return x if pd.notna(id_x) else "INCOHERENT VALUES"

    # Standard checks
    if pd.isna(x) and pd.isna(y):
        return "INCOHERENT VALUES"
    if x == "DESCONOCIDO" and pd.isna(y):
        return "INCOHERENT VALUES"
    if pd.isna(x) and y == "DESCONOCIDO":
        return "INCOHERENT VALUES"
    if x == "DESCONOCIDO" and y == "DESCONOCIDO":
        return "DESCONOCIDO"
    if pd.isna(x):
        return y
    if pd.isna(y):
        return x
    if x == y:
        return x

    # All other cases as incoherent
    return "INCOHERENT VALUES"


def handle_unknown_bodega(merged_ingresos_inventario):
    """
    Handles rows with 'DESCONOCIDO' in the 'bodega' column.

    Args:
        merged_ingresos_inventario (pd.DataFrame): The merged DataFrame with `bodega` and other columns.

    Returns:
        pd.DataFrame: Updated DataFrame after handling 'DESCONOCIDO' values.
        pd.DataFrame: Eligible rows for replacement.
        pd.DataFrame: Rows successfully replaced.
        pd.DataFrame: Remaining rows with 'DESCONOCIDO'.
    """
    base_output_path = get_base_output_path()

    print("Columns in the DataFrame:", merged_ingresos_inventario.columns)

    # Create a filtered DataFrame excluding 'DESCONOCIDO'
    filtered_bodegas = merged_ingresos_inventario[merged_ingresos_inventario['bodega'] != 'DESCONOCIDO']

    # Find replacement bodega for each idcontacto_x where there is exactly one unique bodega
    replacement_bodega = filtered_bodegas.groupby('idcontacto').filter(lambda x: len(x['bodega'].unique()) == 1)
    replacement_bodega = replacement_bodega.groupby('idcontacto')['bodega'].first()

    # Create mask for rows where bodega is 'DESCONOCIDO' and valid replacement exists
    mask = (merged_ingresos_inventario['bodega'] == 'DESCONOCIDO') & \
           merged_ingresos_inventario['idcontacto'].isin(replacement_bodega.index)

    # Save eligible rows for replacement
    eligible_rows = merged_ingresos_inventario.loc[mask]
    eligible_rows_path = os.path.join(base_output_path, 'eligible_rows.csv')
    eligible_rows.to_csv(eligible_rows_path, index=True)

    # Apply the replacement
    merged_ingresos_inventario.loc[mask, 'bodega'] = merged_ingresos_inventario.loc[mask, 'idcontacto'].map(
        replacement_bodega)

    # Save successfully replaced rows
    replaced_rows = merged_ingresos_inventario.loc[mask]
    replaced_rows_path = os.path.join(base_output_path, 'replaced_rows.csv')
    replaced_rows.to_csv(replaced_rows_path, index=True)

    # Save rows that remain unchanged
    remaining_rows = merged_ingresos_inventario[merged_ingresos_inventario['bodega'] == 'DESCONOCIDO']
    remaining_rows_path = os.path.join(base_output_path, 'remaining_rows.csv')
    remaining_rows.to_csv(remaining_rows_path, index=True)

    # Save the entire DataFrame after processing
    final_output_path = os.path.join(base_output_path, 'merged_ingresos_inventario_after_mask.csv')
    merged_ingresos_inventario.to_csv(final_output_path, index=True)

    # # --- Debugging Starts Here ---
    #
    # # Step 1: Check 'bodega' unique values
    # print("Unique values in 'bodega':", merged_ingresos_inventario['bodega'].unique())
    # print("Count of 'DESCONOCIDO' in 'bodega':", (merged_ingresos_inventario['bodega'] == 'DESCONOCIDO').sum())
    #
    # # Step 2: Check replacement bodega
    # print("Replacement Bodega:\n", replacement_bodega)
    #
    # # Step 3: Validate mask
    # print("Mask (rows that should be replaced):\n", mask.sum())  # Count of rows matching the mask
    # if mask.sum() > 0:
    #     print("Rows matching the mask:\n", merged_ingresos_inventario.loc[mask])
    #
    # # Step 4: Check filtered bodega groups
    # filtered_groups = filtered_bodegas.groupby('idcontacto').agg({'bodega': 'nunique'})
    # print("Number of unique 'bodega' values per group:\n", filtered_groups)
    #
    # # Step 5: Check remaining 'DESCONOCIDO' rows
    # print("Remaining rows with 'DESCONOCIDO':\n", remaining_rows)
    #
    # # Step 6: Debug mapping values
    # print("Mapped replacement values for rows in the mask:\n",
    #       merged_ingresos_inventario.loc[mask, 'idcontacto'].map(replacement_bodega))
    #
    # # Save filtered groups for analysis (optional)
    # filtered_groups_path = os.path.join(get_base_output_path(), 'filtered_groups.csv')
    # filtered_groups.to_csv(filtered_groups_path)
    # print(f"Filtered groups saved to: {filtered_groups_path}")

    return merged_ingresos_inventario, eligible_rows, replaced_rows, remaining_rows


# Determinar el área de análisis
def get_base_path():
    if os.name == 'nt':  # Windows
        return r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\tbls22_06_24\\'
    else:  # MacOS (or others)
        return '/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/tbls22_06_24/'


def get_base_output_path():
    if os.name == 'nt':  # Windows
        # obase_path = r'C:\Users\melanie\Downloads'
        obase_path = r'C:\Users\josemaria\Downloads'

    else:  # MacOS (or others)
        obase_path = r'/Users/j.m./Downloads'
    return obase_path


# Main functions -------------------------------------------------------------------------------------------------------

def load_data():
    with Progress() as progress:
        base_path = get_base_path()  # Get the correct base path based on the OS

        # Función para tratar con las líneas problemáticas de los csv, para evitar errores de tokenización
        def read_csv_in_chunks(file_path, chunk_size=10000, encoding='latin1', dtype='str'):
            chunks = []
            try:
                for chunk in pd.read_csv(file_path, encoding=encoding, dtype=dtype, chunksize=chunk_size,
                                         on_bad_lines='skip'):
                    chunks.append(chunk)
            except pd.errors.ParserError as e:
                print(f"Error parsing CSV file: {e}")
            return pd.concat(chunks, ignore_index=True)

        task = progress.add_task("[green]Progress:", total=6)

        # TABLAS DE LAS BODEGAS A, G, E, N.
        cohd_ingresos_mobu = pd.read_csv(
            os.path.join(base_path, 'cohd.csv'), encoding='latin1', dtype='str')
        rpshd_despachos_mobu = pd.read_csv(
            os.path.join(base_path, 'rpshd.csv'), encoding='latin1', dtype='str')
        rpsdt_productos_mobu = read_csv_in_chunks(
            os.path.join(base_path, 'rpsdt.csv'))
        registro_ingresos_mobu = pd.read_csv(
            os.path.join(base_path, 'incompra.csv'), encoding='latin1', dtype='str')
        registro_salidas_mobu = read_csv_in_chunks(
            os.path.join(base_path, 'inmovid.csv'))
        inmovih_table_mobu = pd.read_csv(
            os.path.join(base_path, 'inmovih.csv'), encoding='latin1', dtype='str')
        saldo_inventory_mobu = read_csv_in_chunks(
            os.path.join(base_path, 'insaldo.csv'))
        producto_modelos_mobu = read_csv_in_chunks(
            os.path.join(base_path, 'inmodelo.csv'))
        ctcentro_table_mobu = read_csv_in_chunks(
            os.path.join(base_path, 'ctcentro.csv'))

        # Leer el archivo con formato estándar
        supplier_info_mobu = pd.read_csv(
            os.path.join(base_path, 'incontac.csv'), encoding='latin1', dtype='str')

        dispatched_inventory_mobu = saldo_inventory_mobu

        # Step 1: MOBU Tables
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Verificar los nombres de las columnas correctas en supplier_info1
        # Renombrar las columnas si es necesario
        if 'codigo' in supplier_info_mobu.columns and 'nombre' in supplier_info_mobu.columns:
            supplier_info_mobu = supplier_info_mobu.rename(columns={'codigo': 'idcontacto', 'nombre': 'descrip'})

        # Asegurar que las columnas 'idcontacto' y 'descrip' existan en supplier_info1
        if 'idcontacto' in supplier_info_mobu.columns and 'descrip' in supplier_info_mobu.columns:
            supplier_info_mobu = supplier_info_mobu[['idcontacto', 'descrip']]

        # Crear la nueva columna 'idingreso' con los primeros 10 caracteres de 'idproducto'
        rpsdt_productos_mobu['idingreso'] = rpsdt_productos_mobu['idproducto'].apply(lambda x: x[:10])

        # Asegurar que 'idingreso' sea de tipo string
        rpsdt_productos_mobu['idingreso'] = rpsdt_productos_mobu['idingreso'].astype(str)

        cohd_ingresos_bodc = pd.read_csv(
            os.path.join(base_path, 'cohd_c.csv'), encoding='latin1', dtype='str')
        rpshd_despachos_bodc = pd.read_csv(
            os.path.join(base_path, 'rpshd_c.csv'), encoding='latin1', dtype='str')
        rpsdt_productos_bodc = read_csv_in_chunks(
            os.path.join(base_path, 'rpsdt_c.csv'))
        registro_ingresos_bodc = pd.read_csv(
            os.path.join(base_path, 'incompra_c.csv'), encoding='latin1', dtype='str')
        registro_salidas_bodc = read_csv_in_chunks(
            os.path.join(base_path, 'inmovid_c.csv'))
        inmovih_table_bodc = pd.read_csv(
            os.path.join(base_path, 'inmovih_c.csv'), encoding='latin1', dtype='str')
        saldo_inventory_bodc = read_csv_in_chunks(
            os.path.join(base_path, 'insaldo_c.csv'))
        producto_modelos_bodc = read_csv_in_chunks(
            os.path.join(base_path, 'inmodelo_c.csv'))
        ctcentro_table_bodc = read_csv_in_chunks(
            os.path.join(base_path, 'ctcentro_c.csv'))

        supplier_info_bodc = pd.read_csv(
            os.path.join(base_path, 'incontac_c.csv'), encoding='latin1', dtype='str')

        # Step 2: BODC tables
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        dispatched_inventory_bodc = saldo_inventory_bodc

        # Crear la nueva columna 'idingreso' con los primeros 10 caracteres de 'idproducto'
        rpsdt_productos_bodc['idingreso'] = rpsdt_productos_bodc['idproducto'].apply(lambda x: x[:10])

        # Asegurar que 'idingreso' sea de tipo string
        rpsdt_productos_bodc['idingreso'] = rpsdt_productos_bodc['idingreso'].astype(str)

        # Agregar diferenciador _c a cada idingreso para generar llave unica
        saldo_inventory_bodc['idingreso'] = saldo_inventory_bodc['idingreso'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_ingresos_bodc['idingreso'] = registro_ingresos_bodc['idingreso'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        saldo_inventory_bodc['idcontacto'] = saldo_inventory_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        saldo_inventory_bodc['idcentro'] = saldo_inventory_bodc['idcentro'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        saldo_inventory_bodc['retnum'] = saldo_inventory_bodc['retnum'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        cohd_ingresos_bodc['idcontacto'] = cohd_ingresos_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        cohd_ingresos_bodc['retnum'] = cohd_ingresos_bodc['retnum'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        cohd_ingresos_bodc['numero'] = cohd_ingresos_bodc['numero'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['idingreso'] = registro_salidas_bodc['idingreso'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['idcontacto'] = registro_salidas_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['trannum'] = registro_salidas_bodc['trannum'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['idcentro1'] = registro_salidas_bodc['idcentro1'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['idcentro'] = registro_salidas_bodc['idcentro'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['idmodelo'] = registro_salidas_bodc['idmodelo'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_salidas_bodc['numero'] = registro_salidas_bodc['numero'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_ingresos_bodc['idcontacto'] = registro_ingresos_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_ingresos_bodc['retnum'] = registro_ingresos_bodc['retnum'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        registro_ingresos_bodc['referencia'] = registro_ingresos_bodc['referencia'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        supplier_info_bodc['idcontacto'] = supplier_info_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        ctcentro_table_bodc['idcentro'] = ctcentro_table_bodc['idcentro'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        inmovih_table_bodc['idcontacto'] = inmovih_table_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        inmovih_table_bodc['idcentro'] = inmovih_table_bodc['idcentro'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        inmovih_table_bodc['trannum'] = inmovih_table_bodc['trannum'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        inmovih_table_bodc['referencia'] = inmovih_table_bodc['referencia'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        inmovih_table_bodc['idcentro1'] = inmovih_table_bodc['idcentro1'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpsdt_productos_bodc['idcontacto'] = rpsdt_productos_bodc['idcontacto'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpsdt_productos_bodc['numero'] = rpsdt_productos_bodc['numero'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpsdt_productos_bodc['idingreso'] = rpsdt_productos_bodc['idingreso'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpsdt_productos_bodc['idmodelo'] = rpsdt_productos_bodc['idmodelo'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpshd_despachos_bodc['idcentro1'] = rpshd_despachos_bodc['idcentro1'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpshd_despachos_bodc['idcentro'] = rpshd_despachos_bodc['idcentro'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpshd_despachos_bodc['referencia'] = rpshd_despachos_bodc['referencia'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpshd_despachos_bodc['numero'] = rpshd_despachos_bodc['numero'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)
        rpshd_despachos_bodc['trannum'] = rpshd_despachos_bodc['trannum'].apply(
            lambda x: str(x) + '_c' if pd.notna(x) else x)

        # Step 3: implementing BODC key
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        cohd_ingresos_bode = pd.read_csv(
            os.path.join(base_path, 'cohd_e.csv'), encoding='latin1', dtype='str')
        rpshd_despachos_bode = pd.read_csv(
            os.path.join(base_path, 'rpshd_e.csv'), encoding='latin1', dtype='str')
        rpsdt_productos_bode = read_csv_in_chunks(
            os.path.join(base_path, 'rpsdt_e.csv'))
        registro_ingresos_bode = pd.read_csv(
            os.path.join(base_path, 'incompra_e.csv'), encoding='latin1', dtype='str')
        registro_salidas_bode = read_csv_in_chunks(
            os.path.join(base_path, 'inmovid_e.csv'))
        inmovih_table_bode = pd.read_csv(
            os.path.join(base_path, 'inmovih_e.csv'), encoding='latin1', dtype='str')
        saldo_inventory_bode = read_csv_in_chunks(
            os.path.join(base_path, 'insaldo_e.csv'))
        producto_modelos_bode = read_csv_in_chunks(
            os.path.join(base_path, 'inmodelo_e.csv'))
        ctcentro_table_bode = read_csv_in_chunks(
            os.path.join(base_path, 'ctcentro_e.csv'))

        # Step 4: Loading BODE tables
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        dispatched_inventory_bode = saldo_inventory_bode

        supplier_info_bode = pd.read_csv(
            os.path.join(base_path, 'incontac_e.csv'), encoding='latin1', dtype='str')

        # Crear la nueva columna 'idingreso' con los primeros 10 caracteres de 'idproducto'
        rpsdt_productos_bode['idingreso'] = rpsdt_productos_bode['idproducto'].apply(lambda x: x[:10])

        # Asegurar que 'idingreso' sea de tipo string
        rpsdt_productos_bode['idingreso'] = rpsdt_productos_bode['idingreso'].astype(str)

        # Agregar diferenciador _c a cada idingreso para generar llave unica
        saldo_inventory_bode['idingreso'] = saldo_inventory_bode['idingreso'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_ingresos_bode['idingreso'] = registro_ingresos_bode['idingreso'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        saldo_inventory_bode['idcontacto'] = saldo_inventory_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        saldo_inventory_bode['idcentro'] = saldo_inventory_bode['idcentro'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        saldo_inventory_bode['retnum'] = saldo_inventory_bode['retnum'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        cohd_ingresos_bode['idcontacto'] = cohd_ingresos_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        cohd_ingresos_bode['retnum'] = cohd_ingresos_bode['retnum'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        cohd_ingresos_bode['numero'] = cohd_ingresos_bode['numero'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['idingreso'] = registro_salidas_bode['idingreso'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['idcontacto'] = registro_salidas_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['trannum'] = registro_salidas_bode['trannum'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['idcentro1'] = registro_salidas_bode['idcentro1'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['idcentro'] = registro_salidas_bode['idcentro'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['idmodelo'] = registro_salidas_bode['idmodelo'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_salidas_bode['numero'] = registro_salidas_bode['numero'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_ingresos_bode['idcontacto'] = registro_ingresos_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_ingresos_bode['retnum'] = registro_ingresos_bode['retnum'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        registro_ingresos_bode['referencia'] = registro_ingresos_bode['referencia'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        supplier_info_bode['idcontacto'] = supplier_info_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        ctcentro_table_bode['idcentro'] = ctcentro_table_bode['idcentro'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        inmovih_table_bode['idcontacto'] = inmovih_table_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        inmovih_table_bode['idcentro'] = inmovih_table_bode['idcentro'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        inmovih_table_bode['trannum'] = inmovih_table_bode['trannum'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        inmovih_table_bode['referencia'] = inmovih_table_bode['referencia'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        inmovih_table_bode['idcentro1'] = inmovih_table_bode['idcentro1'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpsdt_productos_bode['idcontacto'] = rpsdt_productos_bode['idcontacto'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpsdt_productos_bode['numero'] = rpsdt_productos_bode['numero'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpsdt_productos_bode['idingreso'] = rpsdt_productos_bode['idingreso'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpsdt_productos_bode['idmodelo'] = rpsdt_productos_bode['idmodelo'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpshd_despachos_bode['idcentro1'] = rpshd_despachos_bode['idcentro1'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpshd_despachos_bode['idcentro'] = rpshd_despachos_bode['idcentro'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpshd_despachos_bode['referencia'] = rpshd_despachos_bode['referencia'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpshd_despachos_bode['numero'] = rpshd_despachos_bode['numero'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)
        rpshd_despachos_bode['trannum'] = rpshd_despachos_bode['trannum'].apply(
            lambda x: str(x) + '_e' if pd.notna(x) else x)

        # Step 5: implementing BODE key
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # print("SAN ANDRES BODE:\n")
        # print("Ingresos Status BODE:\n ", cohd_ingresos_bode.head())
        # print("Despachos Status BODE:\n ", rpshd_despachos_bode.head())
        # print("Despachos-Productos Status BODE:\n ", rpsdt_productos_bode.head())
        # print("Registro Ingresos BODE:\n ", registro_ingresos_bode.head())
        # print("Registro Salidas BODE:\n ", registro_salidas_bode.head())
        # print("Tabla inmovih BODE:\n ", inmovih_table_bode.head())
        # print("Saldo/Inventario BODE:\n ", saldo_inventory_bode.head())
        # print("Contactos BODE:\n ", supplier_info_bode.head())
        # print("CTCENTRO table BODE:\n ", ctcentro_table_bode.head())
        # print("Productos BODE:\n ", producto_modelos_bode.head())

        # Function to concatenate tables with union approach, ensuring output is a DataFrame
        def concatenate_tables_union(table_list, table_name):
            concatenated_df = pd.concat(table_list, axis=0, ignore_index=True, sort=False)
            if not isinstance(concatenated_df, pd.DataFrame):
                raise TypeError(f"The concatenated result for {table_name} is not a DataFrame")
            return concatenated_df

        # Grouping tables as before
        ingresos_tables = [cohd_ingresos_mobu, cohd_ingresos_bodc, cohd_ingresos_bode]
        despachos_tables = [rpshd_despachos_mobu, rpshd_despachos_bodc, rpshd_despachos_bode]
        productos_tables = [rpsdt_productos_mobu, rpsdt_productos_bodc, rpsdt_productos_bode]
        registro_ingresos_tables = [registro_ingresos_mobu, registro_ingresos_bodc, registro_ingresos_bode]
        registro_salidas_tables = [registro_salidas_mobu, registro_salidas_bodc, registro_salidas_bode]
        inmovih_tables = [inmovih_table_mobu, inmovih_table_bodc, inmovih_table_bode]
        saldo_inventory_tables = [saldo_inventory_mobu, saldo_inventory_bodc, saldo_inventory_bode]
        producto_modelos_tables = [producto_modelos_mobu, producto_modelos_bodc, producto_modelos_bode]
        ctcentro_tables = [ctcentro_table_mobu, ctcentro_table_bodc, ctcentro_table_bode]
        supplier_info_tables = [supplier_info_mobu, supplier_info_bodc, supplier_info_bode]
        dispatched_inventory_tables = [dispatched_inventory_mobu, dispatched_inventory_bodc, dispatched_inventory_bode]

        # Use the union approach for concatenation
        wl_ingresos = concatenate_tables_union(ingresos_tables, "Ingresos")
        rpshd_despachos = concatenate_tables_union(despachos_tables, "Despachos")
        rpsdt_productos = concatenate_tables_union(productos_tables, "Productos")
        registro_ingresos = concatenate_tables_union(registro_ingresos_tables, "Registro Ingresos")
        registro_salidas = concatenate_tables_union(registro_salidas_tables, "Registro Salidas")
        inmovih_table = concatenate_tables_union(inmovih_tables, "Inmovih")
        saldo_inventory = concatenate_tables_union(saldo_inventory_tables, "Saldo Inventory")
        producto_modelos = concatenate_tables_union(producto_modelos_tables, "Producto Modelos")
        ctcentro_table = concatenate_tables_union(ctcentro_tables, "Ctcentro")
        supplier_info = concatenate_tables_union(supplier_info_tables, "Supplier Info")
        dispatched_inventory = concatenate_tables_union(dispatched_inventory_tables, "Inventario Despachado")

        inventario_sin_filtro = saldo_inventory

        # Step 6: Concatenating tables
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)
        print("Data loaded correctly.\n")

        # Save the final DataFrame to CSV
        output_path = os.path.join(get_base_output_path(), 'registro_ingresos_load_data.csv')
        registro_ingresos.to_csv(output_path, index=False)


    # print("\nSAN ANDRES DESPUÉS DE CONCATENAR LAS BODEGAS:\n")
    # print("Ingresos Status:\n", wl_ingresos.head(50))
    # print("Despachos Status:\n", rpshd_despachos.head(50))
    # print("Productos Status:\n", rpsdt_productos.head(50))
    # print("Registro Ingresos Status:\n", registro_ingresos.head(50))
    # print("Registro Salidas Status:\n", registro_salidas.head(50))
    # print("Inmovih Status:\n", inmovih_table.head(50))
    # print("Saldo Inventory Status:\n", saldo_inventory.head(50))
    # print("Producto Modelos Status:\n", producto_modelos.head(50))
    # print("Ctcentro Status:\n", ctcentro_table.head(50))
    # print("Supplier Info Status:\n", supplier_info.head(50))

    return (
        wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas, inmovih_table,
        saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory, inventario_sin_filtro
    )


def data_processing(wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos,
                    registro_salidas, inmovih_table, saldo_inventory, supplier_info, ctcentro_table,
                    producto_modelos, dispatched_inventory, inventario_sin_filtro):


    with Progress() as progress:
        task = progress.add_task("[green]Processing Data: ", total=6)

        dispatched_inventory = dispatched_inventory[[
            'idcentro', 'idbodega', 'idingreso', 'itemno', 'idstatus', 'idmodelo', 'idcoldis',
            'fecha', 'ingresa', 'idcontacto', 'retnum', 'idubica', 'pesokgs', 'equipo', 'inicial', 'salidas',
            'idpedido',
            'idubica1',
            'idproducto']]

        # print(" \n Eliminando columnas de rpsdt_productos SA...")
        rpsdt_productos = rpsdt_productos.loc[:,
                          ['numero', 'itemline', 'estatus', 'idproducto', 'idcontacto', 'idmodelo',
                           'idcoldis', 'idubica', 'cantidad', 'equipo', 'idubica1', 'idingreso', 'ingresa']]

        # print(" \n Eliminando columnas de rpshd_despachos SA...")
        rpshd_despachos = rpshd_despachos.loc[:, ['numero', 'estatus', 'tipo', 'fecha', 'idcentro', 'idcentro1',
                                                  'descrip', 'itemcount', 'pzascan', 'trannum', 'equipo']]

        #     print("\n Eliminando columnas de saldo_inventory SA...")
        saldo_inventory = saldo_inventory[[
            'idcentro', 'idbodega', 'idingreso', 'itemno', 'idstatus', 'idmodelo', 'idcoldis',
            'fecha', 'idcontacto', 'retnum', 'idubica', 'pesokgs', 'equipo', 'inicial', 'salidas', 'idubica1',
            'idproducto']]

        #     print("\nEliminando columnas de registro_salidas SA...")
        registro_salidas = registro_salidas[['trannum', 'lineano', 'fecha', 'cantidad', 'idmodelo', 'idcoldis',
                                             'idingreso', 'itemno', 'idcontacto', 'equipo', 'idcentro', 'idcentro1',
                                             'idclase', 'numero']]

        #     print("\nEliminando columnas de registro_ingresos SA...")
        registro_ingresos = registro_ingresos[['idingreso', 'fecha', 'items', 'transtatus', 'descrip', 'available',
                                               'equipo', 'idcontacto', 'retnum']]



        #     print("\nEliminando columnas de wl_ingresos o Ingresos Status SA...")
        wl_ingresos = wl_ingresos[['idcoclase', 'numero', 'itemcount', 'itemqty', 'fecha', 'idcontacto',
                                   'descrip', 'idcostatus', 'retnum', 'equipo']]
        #     print("Columnas resultantes wl_ingresos SA...", wl_ingresos.columns)

        #     print("\nEliminando columnas de inmovih_table SA...")
        inmovih_table = inmovih_table[['idbodega', 'idclase', 'numero', 'fecha', 'idcontacto', 'referencia',
                                       'transtatus', 'descrip', 'trannum', 'linead', 'lineac',
                                       'idcliente', 'equipo', 'idcentro', 'idcentro1']]

        inventario_sin_filtro = inventario_sin_filtro[[
            'idcentro', 'idbodega', 'idingreso', 'itemno', 'idstatus', 'idmodelo', 'idcoldis',
            'fecha', 'modifica', 'ingresa', 'idcontacto', 'retnum', 'idubica', 'pesokgs', 'equipo', 'inicial',
            'salidas',
            'idubica1',
            'idproducto', 'idpedido']]

        #     print("\nEliminando columnas de supplier_info SA...")
        supplier_info = supplier_info[['idcontacto', 'descrip']]

        #     print("\nEliminando columnas de ctcentro SA...")
        ctcentro_table = ctcentro_table[['idcentro', 'descrip']]

        #     print("\nEliminando columnas de productos_modelo SA...")
        producto_modelos = producto_modelos.loc[:, ['idmodelo', 'descrip']]

        # Step 1: Cleaning and removing unnecessary columns
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory = saldo_inventory[saldo_inventory['idstatus'] == '01']

        # # Delete rows with idubica == DESPAC & TIENDA / saldo_inventory
        # print("Eliminando filas 'DESPAC', 'TIENDA' de saldo_inventory SA...")
        # saldo_inventory = saldo_inventory[~saldo_inventory['idubica'].isin(['DESPAC', 'TIENDA'])]
        # print("\nSALDO INVENTORY DESPUES DE ELIMINAR XX, 3 idubica = TIENDA/DESPAC: \n ", saldo_inventory.head(10))

        # Delete rows with estatus == 9 (anuladas) and estatus == 5 (entregadas) / rpshd_despachos
        #     print("\n Eliminando filas 'anuladas' y 'entregadas' de rpshd_despachos SA...")
        pedidos_anulados = rpshd_despachos[rpshd_despachos['estatus'] == '9'].index
        pedidos_entregados = rpshd_despachos[rpshd_despachos['estatus'] == '5'].index
        rpshd_despachos = rpshd_despachos.drop(pedidos_anulados)
        rpshd_despachos = rpshd_despachos.drop(pedidos_entregados)

        # Filtrar tablas de registro_salidas e inmovih_table para que muestre únicamente los despachos (TR01)
        #     print("\n Filtrando únicamente los despachos TR01 en SA...")
        registro_salidas = registro_salidas.loc[registro_salidas['idclase'] == 'TR01']
        inmovih_table = inmovih_table.loc[inmovih_table['idclase'] == 'TR01']

        # Filtrar Dispatched Inventory, status == 3
        #     print("Preparando el Dispatched Inventory SA...")
        dispatched_inventory_rows_delete = dispatched_inventory[
            ~dispatched_inventory['idstatus'].isin(['XX', '03'])].index
        dispatched_inventory = dispatched_inventory.drop(dispatched_inventory_rows_delete)

        # Step 2: Filtering out data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Eliminar ubicaciones de depósito temporal que ya no existen en la actualidad.
        dispatched_inventory_locations_delete = dispatched_inventory[dispatched_inventory['idubica'].isin(
            ['DT1H2', 'DT1I3', 'DT1J3', 'DT2C2', 'DT2G3', 'DT2H1', 'DT3D1', 'DT3E1', 'DT3E1', 'DT3E2', 'DT3I1'])].index
        dispatched_inventory = dispatched_inventory.drop(dispatched_inventory_locations_delete)
        inventario_sin_filtro = inventario_sin_filtro.drop(dispatched_inventory_locations_delete)

        # Eliminar clientes prueba
        ids_to_remove = ['000099', 'AC0001']
        supplier_info = supplier_info[~supplier_info['idcontacto'].isin(ids_to_remove)]

        ids_to_remove_ct = ['002']
        ctcentro_table = ctcentro_table[~ctcentro_table['idcentro'].isin(ids_to_remove_ct)]

        # Step 3: Filtering out unnecessary locations
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Aplicar la función de corrección a las columnas relevantes
        #     print("\n***Aplicando la función de corrección de columnas.***")

        saldo_inventory.loc[:, 'fecha'] = pd.to_datetime(saldo_inventory['fecha'], errors='coerce')
        dispatched_inventory.loc[:, 'fecha'] = pd.to_datetime(dispatched_inventory['fecha'], errors='coerce')
        registro_salidas.loc[:, 'fecha'] = pd.to_datetime(registro_salidas['fecha'], errors='coerce')
        registro_ingresos.loc[:, 'fecha'] = pd.to_datetime(registro_ingresos['fecha'], errors='coerce')
        wl_ingresos.loc[:, 'fecha'] = pd.to_datetime(wl_ingresos['fecha'], errors='coerce')
        inmovih_table.loc[:, 'fecha'] = pd.to_datetime(inmovih_table['fecha'], errors='coerce')
        rpshd_despachos.loc[:, 'fecha'] = pd.to_datetime(rpshd_despachos['fecha'], errors='coerce')



        # Step 4: Correcting data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        def safe_strftime(df, col):
            if df[col].dtype == 'datetime64[ns]':  # Check if column is datetime
                df[col] = df[col].dt.strftime('%Y-%m-%d').fillna('')  # Convert to string and fill NaT
            else:
                print(f"Warning: '{col}' column contains non-datetime values")

        # Apply the safe conversion to all relevant DataFrames
        safe_strftime(saldo_inventory, 'fecha')
        safe_strftime(dispatched_inventory, 'fecha')
        safe_strftime(registro_salidas, 'fecha')
        safe_strftime(registro_ingresos, 'fecha')
        safe_strftime(wl_ingresos, 'fecha')
        safe_strftime(inmovih_table, 'fecha')
        safe_strftime(rpshd_despachos, 'fecha')

        # print("***Aplicación finalizada. Las fechas han sido normalizadas.\n***")

        # Ordenando las fechas de más recientes a más antiguas
        saldo_inventory = saldo_inventory.sort_values(by='fecha', ascending=False)
        dispatched_inventory = dispatched_inventory.sort_values(by='fecha', ascending=False)
        registro_salidas = registro_salidas.sort_values(by='fecha', ascending=False)
        registro_ingresos = registro_ingresos.sort_values(by='fecha', ascending=False)
        wl_ingresos = wl_ingresos.sort_values(by='fecha', ascending=False)
        inmovih_table = inmovih_table.sort_values(by='fecha', ascending=False)
        rpshd_despachos = rpshd_despachos.sort_values(by='fecha', ascending=False)

        # Step 5: Sorting and preparing data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # print("***Las fechas han sido ordenadas de más recientes a más antiguas.\n***")

        # # Printing new data result after dropping off
        # print("\n SAN ANDRES | DATAFRAMES ACTUALIZADOS: \n")
        # print("\n Ingresos Status SA:\n ", wl_ingresos.head(50))
        # print("\n Despachos Status SA: \n ", rpshd_despachos.head(50))
        # print("\n Despachos-Productos Status SA:\n ", rpsdt_productos.head(50))
        # print("\n Registro Ingresos SA: \n", registro_ingresos.head(50))
        # print("\n Registro Salidas SA:\n ", registro_salidas.head(50))
        # print("\n Tabla inmovih SA: \n", inmovih_table.head(50))
        # print("\n Saldo/Inventario SA: \n ", saldo_inventory.head(50))
        # print("\n Contactos SA: \n ", supplier_info.head(50))
        # print("\n CTCENTRO table SA: \n", ctcentro_table.head(50))
        # print("\n Dispatched_inventory  SA: \n", dispatched_inventory.head(50))

        saldo_inventory_cnan = ['idcentro', 'idbodega', 'idingreso', 'itemno', 'idstatus', 'idmodelo',
                                'idcoldis', 'fecha', 'idcontacto', 'retnum', 'idubica', 'pesokgs',
                                'equipo', 'inicial', 'salidas', 'idubica1', 'idproducto']

        saldo_inventory.loc[:, saldo_inventory_cnan] = saldo_inventory.loc[:, saldo_inventory_cnan].fillna("")
        saldo_inventory.loc[:, 'idubica'] = saldo_inventory.loc[:, 'idubica'].fillna("Ubicación Desconocida")
        saldo_inventory.loc[:, 'idubica1'] = saldo_inventory.loc[:, 'idubica1'].fillna("Tarima Desconocida")

        inventario_sin_filtro.loc[:, saldo_inventory_cnan] = inventario_sin_filtro.loc[:, saldo_inventory_cnan].fillna(
            "")
        inventario_sin_filtro.loc[:, 'idubica'] = inventario_sin_filtro.loc[:, 'idubica'].fillna(
            "Ubicación Desconocida")
        inventario_sin_filtro.loc[:, 'idubica1'] = inventario_sin_filtro.loc[:, 'idubica1'].fillna("Tarima Desconocida")

        rpsdt_productos_cnan = ['numero', 'itemline', 'estatus', 'idproducto', 'idcontacto', 'idmodelo', 'idcoldis',
                                'cantidad',
                                'idubica'
                                ]
        rpsdt_productos.loc[:, rpsdt_productos_cnan] = rpsdt_productos[rpsdt_productos_cnan].fillna("")

        rpsdt_productos['idubica1'] = rpsdt_productos['idubica1'].apply(
            lambda x: "DESCONOCIDO" if pd.isna(x) or str(x).strip() == "" else x)
        rpsdt_productos['idubica'] = rpsdt_productos['idubica'].apply(
            lambda x: "DESCONOCIDO" if pd.isna(x) or str(x).strip() == "" else x)

        # Step 6: Filling NaNs
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("Data Processing completed successfully.\n")
    return (wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
            inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory,
            inventario_sin_filtro)


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

    print("Data Screening completed successfully.\n")

    output_path = os.path.join(get_base_output_path(), 'registro_ingresos_data_screening.csv')
    registro_ingresos.to_csv(output_path, index=False)


    return (saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos, rpshd_despachos, wl_ingresos,
            inmovih_table, dispatched_inventory)


def insaldo_bode_comp(saldo_inventory):
    # Leer los datos especificando los tipos de datos
    if os.name == 'nt':
        inmodelo_clasificacion = pd.read_excel(
            r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\varios\modelos_clasificacion.xlsx')
    else:
        inmodelo_clasificacion = pd.read_excel(
            r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')

    # Step 2: Ensure `idmodelo` columns are of the same type (e.g., string)
    saldo_inventory['idmodelo'] = saldo_inventory['idmodelo'].astype(str)
    inmodelo_clasificacion['idmodelo'] = inmodelo_clasificacion['idmodelo'].astype(str)

    # Optional: Strip leading/trailing spaces from `idmodelo`
    saldo_inventory['idmodelo'] = saldo_inventory['idmodelo'].str.strip()
    inmodelo_clasificacion['idmodelo'] = inmodelo_clasificacion['idmodelo'].str.strip()

    # Ensure 'inicial' is a float or numeric type
    saldo_inventory['inicial'] = pd.to_numeric(saldo_inventory['inicial'], errors='coerce')
    inmodelo_clasificacion['cubicaje'] = pd.to_numeric(inmodelo_clasificacion['cubicaje'], errors='coerce')

    # Step 1: Separate BODE rows
    bode_inventory = saldo_inventory[saldo_inventory['bodega'] == 'BODE'].copy()

    # Merge to include 'cubicaje' for BODE rows
    bode_merged = bode_inventory.merge(inmodelo_clasificacion[['idmodelo', 'cubicaje']], on='idmodelo', how='left')

    # Step 2: Check for rows where 'inicial' is still NaN (i.e., no match found)
    missing_inicial = bode_merged[bode_merged['inicial'].isna()]

    # Optionally, check if these unmatched idmodelo exist in models_clasificacion
    unmatched_idmodelos = missing_inicial['idmodelo'].unique()

    # Rename 'cubicaje' to 'inicial' in the merged DataFrame
    bode_merged['inicial'] = bode_merged['cubicaje']

    # Drop unnecessary columns if needed
    bode_merged = bode_merged[saldo_inventory.columns]  # Ensure it has the same columns as saldo_inventory

    # Step 2: Remove original BODE rows from saldo_inventory
    saldo_inventory = saldo_inventory[saldo_inventory['bodega'] != 'BODE']

    # Step 3: Append updated BODE rows back to saldo_inventory
    saldo_inventory = pd.concat([saldo_inventory, bode_merged], ignore_index=True)

    default_cubicaje = 1.5  # this is just so we evade errors (NEW PRODUCTS MUST BE ASSIGNED CBM)
    saldo_inventory['inicial'] = saldo_inventory['inicial'].fillna(default_cubicaje)

    output_path = os.path.join(get_base_output_path(), 'insaldo_bode_comp.csv')
    saldo_inventory.to_csv(output_path, index=True)

    return saldo_inventory


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

        # Step: Printing CSV data
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        output_path = os.path.join(get_base_output_path(), 'resumen_mensual_ingresos_fact.csv')
        resumen_mensual_ingresos_fact.to_csv(output_path, index=True)
        output_path = os.path.join(get_base_output_path(), 'resumen_mensual_ingresos_sd.csv')
        resumen_mensual_ingresos_sd.to_csv(output_path, index=True)

    print("Historic inflow of CBM, pallets and units by client and warehouse:\n", resumen_mensual_ingresos_clientes)
    print("Monthly reception data processed correctly.\n")

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
    print("Historic outflow of CBM, pallets and units by client and warehouse:\n",
          resumen_mensual_despachos_clientes_grouped)
    print("Monthly shipment data processed correctly.\n")

    return resumen_mensual_despachos_clientes_grouped, merged_despachos_inventario, resumen_despachos_cliente_fact


def group_by_month_bodega(resumen_mensual_ingresos_clientes, resumen_mensual_despachos_clientes, start_date,
                          end_date):
    print("\n*** Final monthly inflow and outflow dataframes by warehouse ***\n")

    # Ensure 'fecha_x' is in datetime format
    resumen_mensual_ingresos_clientes['fecha'] = pd.to_datetime(resumen_mensual_ingresos_clientes['fecha_x'],
                                                                errors='coerce')
    resumen_mensual_despachos_clientes['fecha'] = pd.to_datetime(resumen_mensual_despachos_clientes['fecha_x'],
                                                                 errors='coerce')

    # Save the final DataFrame to CSV
    output_path = os.path.join(get_base_output_path(), 'resumen_historico_ingresos_clientes.csv')
    resumen_mensual_ingresos_clientes.to_csv(output_path, index=False)
    output_path = os.path.join(get_base_output_path(), 'resumen_historico_despachos_clientes.csv')
    resumen_mensual_despachos_clientes.to_csv(output_path, index=False)

    # Filter data within the date range
    resumen_mensual_ingresos_clientes = resumen_mensual_ingresos_clientes[
        (resumen_mensual_ingresos_clientes['fecha'] >= start_date) & (
                resumen_mensual_ingresos_clientes['fecha'] <= end_date)
        ]
    resumen_mensual_despachos_clientes = resumen_mensual_despachos_clientes[
        (resumen_mensual_despachos_clientes['fecha'] >= start_date) & (
                resumen_mensual_despachos_clientes['fecha'] <= end_date)
        ]

    # Save the final DataFrame to CSV
    output_path = os.path.join(get_base_output_path(), 'resumen_mensual_ingresos_clientes.csv')
    resumen_mensual_ingresos_clientes.to_csv(output_path, index=False)

    if 'Bodega' in resumen_mensual_ingresos_clientes.columns:
        # Check if column values in 'Bodega' start with 'B'
        if resumen_mensual_ingresos_clientes['Bodega'].astype(str).str.startswith('B').any():
            resumen_mensual_ingresos_clientes.rename(columns={'Bodega': 'bodega'}, inplace=True)

    # Group by 'year_month' and 'Bodega' and then sum the relevant columns
    grouped_resumen_mensual_ingresos = resumen_mensual_ingresos_clientes.groupby(['bodega']).agg(
        {'CBM': 'sum', 'Pallets': 'sum', 'Unidades': 'sum'}).reset_index()

    grouped_resumen_mensual_despachos = resumen_mensual_despachos_clientes.groupby(['Bodega']).agg(
        {'CBM': 'sum', 'Pallets': 'sum', 'Unidades': 'sum'}).reset_index()

    # Sorting by values in descending order
    print(f"\nGrouped summary - inflow of CBM by Warehouse for selected month:\n",
          grouped_resumen_mensual_ingresos.sort_values(by='CBM', ascending=False))
    print("\nGrouped summary - outflow of CBM by Warehouse for selected month:\n",
          grouped_resumen_mensual_despachos.sort_values(by='CBM', ascending=False))

    return grouped_resumen_mensual_ingresos, grouped_resumen_mensual_despachos


def capacity_measured_in_cubic_meters(saldo_inventory, supplier_info):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Analyzing Client Inventory: ", total=10)
        # Leer los datos especificando los tipos de datos
        if os.name == 'nt':
            inmodelo_clasificacion = pd.read_excel(
                r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\varios\modelos_clasificacion.xlsx')
        else:
            inmodelo_clasificacion = pd.read_excel(
                r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory = saldo_inventory[saldo_inventory['idstatus'] == '01']

        saldo_inventory['fecha'] = pd.to_datetime(saldo_inventory['fecha'])
        # Ordenar fechas de más reciente a más antiguas
        saldo_inventory = saldo_inventory.sort_values(by='fecha', ascending=False)

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

        # Drop the specified columns
        saldo_inventory = saldo_inventory.drop(columns=['descrip', 'clasificacion', 'cubicaje'])

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inv_cliente_fact = saldo_inventory

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

        saldo_inventory_summed_bodega = saldo_inventory.groupby([
            'bodega',
            'idcontacto'
        ]).agg({'inicial': 'sum', 'idmodelo': 'count',
                'pesokgs': 'sum'}).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory_summed_bodega = saldo_inventory_summed_bodega.drop(columns=[
            'idcontacto',
        ])

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns
        saldo_inventory_summed_bodega.rename(columns={
            'bodega': 'Bodega',
            'pesokgs': 'Unidades',
            'inicial': 'CBM',
            'idmodelo': 'Pallets',
        }, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("\nActual Client Inventory Status by Warehouse:\n", saldo_inventory_summed_bodega)
    print("Clients Inventory data analyzed correctly.\n")

    return saldo_inv_cliente_fact


def billing_data_reconstruction(saldo_inv_cliente_fact, resumen_mensual_ingresos_fact, resumen_despachos_cliente_fact,
                                start_date, end_date, registro_ingresos, supplier_info):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Processing and analyzing Client's operational Data: ", total=44)

        resumen_mensual_ingresos_fact['fecha_x'] = pd.to_datetime(resumen_mensual_ingresos_fact['fecha_x'])
        resumen_despachos_cliente_fact['fecha_x'] = pd.to_datetime(resumen_despachos_cliente_fact['fecha_x'])
        resumen_mensual_ingresos_fact['ddma'] = resumen_mensual_ingresos_fact['ddma'].fillna("")

        print("'idcontacto' in supplier_info:", 'idcontacto' in supplier_info.columns)

        supplier_info = supplier_info.loc[:,['idcontacto', 'descrip']]

        supplier_info['idcontacto'] = supplier_info['idcontacto'].fillna("")
        supplier_info['descrip'] = supplier_info['descrip'].fillna("")

        # Step 3: Rename columns as needed
        supplier_info.rename(columns={
            'descrip': 'Client'
        }, inplace=True)

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
        inflow_with_mode['mode_count'].fillna(1, inplace=True)

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
        inflow_with_mode.rename(columns={
            'fecha_x': 'Date',
            'descrip': 'Description',
            'Bodega': 'Warehouse',
            'inicial': 'CBM',
            'pesokgs': 'Weight or Units',
            'pallets_final': 'Pallets'
        }, inplace=True)

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

        # Final step: Write the cleaned outflow data to CSV or display as needed
        output_path = os.path.join(get_base_output_path(), 'final_inflow_df_fact.csv')
        inflow_grouped.to_csv(output_path, index=False)

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

        output_path = os.path.join(get_base_output_path(), 'saldo_inv_cliente_fact.csv')
        saldo_inv_cliente_fact.to_csv(output_path, index=False)
        output_path = os.path.join(get_base_output_path(), 'filtered_df.csv')
        filtered_df.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Step 2: Create a 'pallets' column with value 1 for each row
        filtered_df['pallets'] = 1

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

        final_df.rename(columns={
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

        }, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        output_path = os.path.join(get_base_output_path(), 'final_inventory_dataframe.csv')
        final_df.to_csv(output_path, index=False)

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

        output_path = os.path.join(get_base_output_path(), 'grouped_by_idubica1_saldo.csv')
        grouped_by_idubica1_saldo.to_csv(output_path, index=False)

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
        outflow_with_mode['mode_count'].fillna(1, inplace=True)

        # Load the 'Unique Modes per Product - KC' data
        unique_modes_file_path = \
            r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\assets\inventory_analysis_client\pallet_mode_KC.xlsx'
        unique_modes_df = pd.read_excel(unique_modes_file_path)

        # unique_modes_file_path = \
        #     (r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU -'
        #      r' OPL/assets/inventory_analysis_client/pallet_mode_KC.xlsx')
        # unique_modes_df = pd.read_excel(unique_modes_file_path)

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

        output_path = os.path.join(get_base_output_path(), 'outflow_with_mode_before_merge.csv')
        outflow_with_mode.to_csv(output_path, index=False)

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
        print("Data types:")
        print("outflow_with_mode['idingreso']: ", outflow_with_mode['idingreso'].dtype)
        print("registro_ingresos['idingreso']: ", registro_ingresos['idingreso'].dtype)

        duplicates = registro_ingresos[registro_ingresos.duplicated(subset='idingreso', keep=False)]
        print("Duplicate idingreso in registro_ingresos:\n", duplicates)

        print("registro_ingresos['descrip'] sample values:\n", registro_ingresos['descrip'].head())
        missing_descrip = registro_ingresos[registro_ingresos['descrip'].isna()]
        print(f"Rows in registro_ingresos with missing descrip: {len(missing_descrip)}")

        outflow_with_mode['idingreso'] = outflow_with_mode['idingreso'].astype(str).str.strip().str.zfill(10)
        registro_ingresos['idingreso'] = registro_ingresos['idingreso'].astype(str).str.strip().str.zfill(10)

        missing_keys = outflow_with_mode.loc[
            ~outflow_with_mode['idingreso'].isin(registro_ingresos['idingreso']), 'idingreso']
        print("Missing idingreso values (not found in registro_ingresos):")
        print(missing_keys.unique())

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

        # Summarize the results
        print("Merge indicator summary:")
        print(outflow_with_mode['_merge'].value_counts())

        # Rows that didn't match
        unmatched_rows = outflow_with_mode[outflow_with_mode['_merge'] == 'left_only']
        print("Rows with unmatched idingreso:")
        print(unmatched_rows[['idingreso']])

        output_path = os.path.join(get_base_output_path(), 'outflow_with_mode_after_merge.csv')
        outflow_with_mode.to_csv(output_path, index=False)

        output_path = os.path.join(get_base_output_path(), 'registro_ingresos_test.csv')
        registro_ingresos.to_csv(output_path, index=False)

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
            'idcontacto':'first',
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
        outflow_grouped.rename(columns={
            'fecha_x': 'Shipping_Date',
            'fecha_y': 'Arrival_Date',
            'pesokgs': 'Weight or Units',
            'calculated_pallets': 'Pallets',
            'cantidad': 'CBM',
            'idmodelo_x': 'idmodelo',
            # 'idcontacto_x': 'idcontacto',
            'descrip': 'Description',
            'bodega': 'Warehouse'
        }, inplace=True)

        print("outflow_grouped: look here", outflow_grouped.head())


        outflow_grouped = outflow_grouped.loc[:,
                          ['trannum', 'idmodelo', 'Arrival_Date', 'Shipping_Date', 'Days', 'Description', 'idcontacto',
                           'Client','CBM',
                           'Pallets',
                           'Weight or Units',
                           'Warehouse']]

        # Write the cleaned outflow data to CSV
        output_path = os.path.join(get_base_output_path(), 'final_outflow_df_fact.csv')
        outflow_grouped.to_csv(output_path, index=False)

        output_path = os.path.join(get_base_output_path(), 'inflow_with_mode_historical.csv')
        inflow_with_mode_historical.to_csv(output_path, index=False)
        output_path = os.path.join(get_base_output_path(), 'outflow_with_mode_historical.csv')
        outflow_with_mode_historical.to_csv(output_path, index=False)

        output_path = os.path.join(get_base_output_path(), 'inflow_with_mode.csv')
        inflow_with_mode.to_csv(output_path, index=False)
        output_path = os.path.join(get_base_output_path(), 'outflow_with_mode.csv')
        outflow_with_mode.to_csv(output_path, index=False)

        output_path = os.path.join(get_base_output_path(), 'final_df.csv')
        final_df.to_csv(output_path, index=False)

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
    print(
        "WARNING: The issue arises because 9,872 idingreso values in outflow_with_mode do not exist in registro_ingresos. "
        "These rows will naturally have None in the descrip column after the left join because there is no corresponding match. \n")
    print("\nFinal Inflow dataframe:\n", inflow_grouped)
    print("\nFinal Outflow DataFrame:\n", outflow_grouped)
    print("\nFinal inventory dataframe:\n", final_df)

    print("Clients Operational data reconstructed successfully.\n")

    return inflow_with_mode_historical, outflow_with_mode_historical, final_df


def inventory_proportions_by_product(saldo_inventory, supplier_info):
    with Progress() as progress:
        # Add a task with a total number of steps
        task = progress.add_task("[green]Clustering Clients inventory data: ", total=14)
        # Leer los datos especificando los tipos de datos
        if os.name == 'nt':
            inmodelo_clasificacion = pd.read_excel(
                r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\varios\modelos_clasificacion.xlsx')
        else:
            inmodelo_clasificacion = pd.read_excel(
                r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - '
                r'OPL/varios/modelos_clasificacion.xlsx')

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
    print("Clustering process complete.\n")


def inventory_oldest_products(saldo_inventory, supplier_info):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Analyzing days on hand: ", total=5)
        # Leer los datos especificando los tipos de datos
        if os.name == 'nt':
            inmodelo_clasificacion = pd.read_excel(
                r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\varios\modelos_clasificacion.xlsx')
        else:
            inmodelo_clasificacion = pd.read_excel(
                r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        saldo_inventory = saldo_inventory[saldo_inventory['idstatus'] == '01']

        saldo_inventory['fecha'] = pd.to_datetime(saldo_inventory['fecha']).dt.date

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

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
            'fecha'
        ]).agg({'idcoldis': 'first', 'inicial': 'sum',
                'pesokgs': 'sum', 'itemno': 'count'}).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns
        saldo_inventory_grouped.rename(columns={
            'fecha': 'Date',
            'pesokgs': 'Units',
            'inicial': 'CBM',
            'idmodelo': 'ProductID',
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

        # Convert current date to pandas Timestamp
        current_date = pd.Timestamp(datetime.now())

        # Ensure the 'Date' column is of type datetime (just in case)
        saldo_inventory_grouped['Date'] = pd.to_datetime(saldo_inventory_grouped['Date'])

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate 'days_in_inventory'
        saldo_inventory_grouped['Days in inventory'] = (current_date - saldo_inventory_grouped['Date']).dt.days

        # Reorder columns to move 'days_in_inventory' next to 'Date'
        cols = ['Date', 'Days in inventory'] + [col for col in saldo_inventory_grouped.columns if
                                                col not in ['Date', 'Days in inventory']]
        saldo_inventory_grouped = saldo_inventory_grouped[cols]

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Sort by date (oldest first)
        saldo_inventory_grouped = saldo_inventory_grouped.sort_values(by='Date', ascending=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    print("\nActual Client inventory oldest products:\n", saldo_inventory_grouped)
    print("Days on hand analysis complete.\n")


def reconstruct_inventory_over_time(
        inflow_with_mode_historical,
        outflow_with_mode_historical,
        start_date=None,
        end_date=None,
        initial_inventory=None
):
    with Progress() as progress:
        # Add a new task
        task = progress.add_task("[green]Reconstructing inventory behavior Data: ", total=20)

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

        # Do the same for outflow_with_mode_historical if 'idingreso' and 'itemno_x' exist
        if 'idingreso' in outflow_with_mode_historical.columns and 'itemno' in outflow_with_mode_historical.columns:
            outflow_with_mode_historical['idingreso'] = outflow_with_mode_historical['idingreso'].astype(str)
            outflow_with_mode_historical['itemno'] = outflow_with_mode_historical['itemno'].astype(str)
            outflow_with_mode_historical['dup_key'] = outflow_with_mode_historical['idingreso'] + \
                                                      outflow_with_mode_historical[
                                                          'itemno']
        else:
            # If the columns are named differently, adjust accordingly
            # For example, if they are 'idingreso_x' and 'itemno_x':
            outflow_with_mode_historical['idingreso'] = outflow_with_mode_historical['idingreso'].astype(str)
            outflow_with_mode_historical['itemno_x'] = outflow_with_mode_historical['itemno_x'].astype(str)
            outflow_with_mode_historical['dup_key'] = outflow_with_mode_historical['idingreso'] + \
                                                      outflow_with_mode_historical[
                                                          'itemno_x']

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
        date_df = pd.DataFrame({'date': date_range})

        if 'idingreso' in inflow_with_mode_historical.index.names:
            inflow_with_mode_historical.reset_index(drop=True, inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Aggregate daily inflows
        daily_inflows = inflow_with_mode_historical.groupby(['fecha_x', 'idcontacto', 'idingreso', 'idmodelo']).agg({
            'inicial': 'sum',
            'pesokgs': 'sum',
            'pallets_final': 'first'  # no. de palets
        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        daily_inflows = daily_inflows.groupby(['fecha_x', 'idcontacto', ]).agg({
            'inicial': 'sum',
            'pesokgs': 'sum',
            'pallets_final': 'sum'  # no. de palets

        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        daily_inflows.rename(columns={
            'fecha_x': 'date',
            'inicial': 'Inflow (CBM)',
            'pesokgs': 'Units inflow',
            'pallets_final': 'Pallets inflow'
        }, inplace=True)

        # Aggregate daily outflows
        daily_outflows = outflow_with_mode_historical.groupby(['fecha_x', 'idcontacto', 'trannum', 'idmodelo_x']).agg(
            {
                'cantidad': 'sum',
                'pesokgs': 'sum',
                'calculated_pallets': 'first'
            }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        daily_outflows = daily_outflows.groupby(['fecha_x', 'idcontacto']).agg({
            'cantidad': 'sum',
            'pesokgs': 'sum',
            'calculated_pallets': 'sum'
        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        daily_outflows.rename(columns={
            'fecha_x': 'date',
            'cantidad': 'Outflow (CBM)',
            # 'idcontacto_x': 'idcontacto',
            'pesokgs': 'Units outflow',
            'calculated_pallets': 'Pallets outflow'
        }, inplace=True)

        # Prepare clients list
        clients = pd.concat([
            daily_inflows[['idcontacto']],
            daily_outflows[['idcontacto']]
        ]).drop_duplicates()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Cross join clients with date range
        inventory_over_time = clients.merge(date_df, how='cross')

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Merge inflows and outflows
        inventory_over_time = inventory_over_time.merge(
            daily_inflows,
            on=['date', 'idcontacto'],
            how='left'
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        inventory_over_time = inventory_over_time.merge(
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

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Merge initial inventory
        inventory_over_time = inventory_over_time.merge(
            initial_inventory,
            on='idcontacto',
            how='left'
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Calculate cumulative inventory levels with adjustments to prevent negative inventory
        for idcontacto, group in inventory_over_time.groupby('idcontacto'):
            initial_inv = group['initial_inventory'].iloc[0]
            # Initialize Units and Pallets with initial inventory if available
            initial_units = 0.0  # Adjust if you have initial units per client
            initial_pallets = 0.0  # Adjust if you have initial pallets per client

            inventory_levels = []
            units_levels = []
            pallets_levels = []
            current_inventory = initial_inv
            current_units = initial_units
            current_pallets = initial_pallets

            for idx, row in group.iterrows():
                inflow = row['Inflow (CBM)']
                outflow = row['Outflow (CBM)']
                units_inflow = row['Units inflow']
                units_outflow = row['Units outflow']
                pallets_inflow = row['Pallets inflow']
                pallets_outflow = row['Pallets outflow']

                # Calculate potential new inventory levels
                potential_inventory = current_inventory + inflow - outflow

                if potential_inventory < 0:
                    # Adjust outflow to prevent negative inventory
                    adjusted_outflow = current_inventory + inflow
                    # Calculate adjustment factor to proportionally adjust units and pallets
                    if outflow != 0:
                        adjustment_factor = adjusted_outflow / outflow
                    else:
                        adjustment_factor = 0.0
                    # Adjust units_outflow and pallets_outflow proportionally
                    adjusted_units_outflow = units_outflow * adjustment_factor
                    adjusted_pallets_outflow = pallets_outflow * adjustment_factor

                    # Update current inventory, units, and pallets
                    current_inventory = 0.0
                    current_units += units_inflow - adjusted_units_outflow
                    current_pallets += pallets_inflow - adjusted_pallets_outflow
                else:
                    # No adjustment needed
                    adjusted_units_outflow = units_outflow
                    adjusted_pallets_outflow = pallets_outflow

                    current_inventory = potential_inventory
                    current_units += units_inflow - adjusted_units_outflow
                    current_pallets += pallets_inflow - adjusted_pallets_outflow

                # Ensure units and pallets are non-negative
                current_units = max(current_units, 0.0)
                current_pallets = max(current_pallets, 0.0)

                # Set units and pallets to zero when inventory level is zero
                if current_inventory == 0.0:
                    current_units = 0.0
                    current_pallets = 0.0

                inventory_levels.append(current_inventory)
                units_levels.append(current_units)
                pallets_levels.append(current_pallets)

            inventory_over_time.loc[group.index, 'Inventory level (CBM)'] = inventory_levels
            inventory_over_time.loc[group.index, 'Units'] = units_levels
            inventory_over_time.loc[group.index, 'Pallets'] = pallets_levels

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Drop the 'initial_inventory' column if not needed
        inventory_over_time.drop(columns=['initial_inventory', 'idcontacto'], inplace=True)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Compute Opening Inventory level (CBM)
        inventory_over_time['Opening Inventory level (CBM)'] = (
                inventory_over_time['Inventory level (CBM)'] - inventory_over_time['Inflow (CBM)'] +
                inventory_over_time[
                    'Outflow (CBM)']
        )

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Group by month and calculate initial and final inventory levels
        inventory_ot_by_month = inventory_over_time.groupby(pd.Grouper(key='date')).agg({
            'Inflow (CBM)': 'sum',
            'Units inflow': 'sum',
            'Pallets inflow': 'sum',
            'Outflow (CBM)': 'sum',
            'Units outflow': 'sum',
            'Pallets outflow': 'sum',
            'Opening Inventory level (CBM)': 'first',  # Initial inventory
            'Inventory level (CBM)': 'last',  # Final inventory
        }).reset_index()

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

        # Rename the columns
        inventory_ot_by_month.rename(columns={
            'Opening Inventory level (CBM)': 'Initial Inventory level (CBM)',
            'Inventory level (CBM)': 'Final Inventory level (CBM)'
        }, inplace=True)

        # # Normalize 'start_date' and 'end_date'
        # start_date = pd.to_datetime(start_date).normalize()
        # end_date = pd.to_datetime(end_date).normalize()

        # # Apply filtering
        # inventory_over_time_filtered = inventory_over_time[
        #     (inventory_over_time['date'] >= start_date) &
        #     (inventory_over_time['date'] <= end_date)
        #     ]

        # Save to CSV
        output_path = os.path.join(get_base_output_path(), 'insaldo_historic_dataframe_behavior_by_month.csv')
        inventory_ot_by_month.to_csv(output_path, index=False)
        output_path = os.path.join(get_base_output_path(), 'inventory_over_time.csv')
        inventory_over_time.to_csv(output_path, index=False)

        # Step:
        time.sleep(1)  # Simulate a task
        progress.update(task, advance=1)

    # print('Inventory behavior for selected month:\n', inventory_over_time)
    print("Inventory behavior reconstruction complete.\n")

    return inventory_over_time, inventory_ot_by_month


def filtering_historic_insaldo(inventory_over_time, start_date, end_date):
    # Ensure the 'date' column is in datetime format
    inventory_over_time['date'] = pd.to_datetime(inventory_over_time['date'])

    # Filter data within the date range
    selected_data = inventory_over_time[
        (inventory_over_time['date'] >= start_date) & (inventory_over_time['date'] <= end_date)
        ]

    selected_month_data = selected_data.drop(columns=['Opening Inventory level (CBM)'])

    # Display the filtered dataframe
    print("\nInventory behavior for selected date range:\n", selected_month_data)

    output_path = os.path.join(get_base_output_path(), 'insaldo_historic_dataframe_last_month.csv')
    selected_month_data.to_csv(output_path, index=True)

    return selected_month_data


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
    print("KPI calculation complete.\n")


def main():
    # Input date range
    start_date_str = input("Enter the start date of analysis (dd/mm/yy or dd-mm-yy): ")
    end_date_str = input("Enter the end date of analysis (dd/mm/yy or dd-mm-yy): ")

    # Convert to datetime with error handling
    try:
        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        if start_date > end_date:
            print("Error: Start date must be before or equal to end date.")
            return

        # Convert to pandas Timestamp for compatibility with DataFrame date columns
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
    except ValueError as e:
        print(e)
        return

    print(f"Analysis from {start_date.date()} to {end_date.date()}")

    pd.set_option(
        "display.max_rows", 100,
        "display.max_columns", None,
        "display.expand_frame_repr", False
    )

    print("\nMain: Loading data...\n")

    (wl_ingresos, rpshd_despachos, rpsdt_productos,
     registro_ingresos, registro_salidas, inmovih_table, saldo_inventory,
     supplier_info, ctcentro_table, producto_modelos, dispatched_inventory, inventario_sin_filtro) = load_data()

    # Debugging: Print loaded data
    print("\nLoaded Data:\n")
    for name, df in zip([
        'wl_ingresos', 'rpshd_despachos', 'rpsdt_productos',
        'registro_ingresos', 'registro_salidas', 'inmovih_table',
        'saldo_inventory', 'supplier_info', 'ctcentro_table',
        'producto_modelos', 'dispatched_inventory', 'inventario_sin_filtro'
    ], [
        wl_ingresos, rpshd_despachos, rpsdt_productos,
        registro_ingresos, registro_salidas, inmovih_table,
        saldo_inventory, supplier_info, ctcentro_table,
        producto_modelos, dispatched_inventory, inventario_sin_filtro
    ]):
        print(f"{name}:\n", df.head(), "\n")

    # Convert 'descrip' and 'idcontacto' to string and strip whitespaces
    supplier_info['descrip'] = supplier_info['descrip'].astype(str).str.strip()
    supplier_info['idcontacto'] = supplier_info['idcontacto'].astype(str).str.strip()

    # Ask user if the analysis should be by Client or by Warehouse
    analysis_type = input("Would you like to analyze by Client or Warehouse? "
                          "(Enter 'C' for Client, 'W' for Warehouse): ").strip().upper()

    if analysis_type == 'C':
        # Display the list of clients
        unique_clients = supplier_info[['idcontacto', 'descrip']].drop_duplicates().reset_index(drop=True)
        print("List of clients:")
        for idx, row in unique_clients.iterrows():
            print(f"{idx}: {row['descrip']} ({row['idcontacto']})")

        # Prompt the user to select the client by entering the index number
        try:
            selected_idx = int(input("Enter the number of the client you want to analyze data by: "))
            if selected_idx < 0 or selected_idx >= len(unique_clients):
                print(f"Invalid selection '{selected_idx}'")
                return
        except ValueError:
            print("Invalid input. Please enter a valid number.")
            return

        selected_entity = unique_clients.iloc[selected_idx]
        entity_id = selected_entity['idcontacto']
        entity_name = selected_entity['descrip']
        print(f"Selected client: {entity_name} (idcontacto: {entity_id})")

        # List of DataFrames to filter by client
        dataframes_to_filter = [wl_ingresos, rpshd_despachos, rpsdt_productos,
                                registro_ingresos, registro_salidas, inmovih_table, saldo_inventory,
                                dispatched_inventory, inventario_sin_filtro]

        # Filter DataFrames based on the selected client before data_processing
        filtered_dataframes = filter_dataframes_by_idcontacto(dataframes_to_filter, entity_id)

        # Debugging: Print filtered data
        print("\nFiltered Data by Client:\n")
        for name, df in zip([
            'wl_ingresos', 'rpshd_despachos', 'rpsdt_productos',
            'registro_ingresos', 'registro_salidas', 'inmovih_table',
            'saldo_inventory', 'dispatched_inventory', 'inventario_sin_filtro'
        ], filtered_dataframes):
            print(f"{name} (Filtered by Client):\n", df.head(), "\n")

        # Unpack filtered DataFrames
        (wl_ingresos, rpshd_despachos, rpsdt_productos,
         registro_ingresos, registro_salidas, inmovih_table, saldo_inventory,
         dispatched_inventory, inventario_sin_filtro) = filtered_dataframes

    elif analysis_type == 'W':
        # Display the list of warehouses
        warehouses = ["BODA", "BODC", "BODE", "BODG", "BODJ", "OPL", "INCOHERENT VALUES", "DESCONOCIDO", "INTEMPERIE",
                      "PISO"]
        print("List of warehouses:")
        for idx, warehouse in enumerate(warehouses):
            print(f"{idx}: {warehouse}")

        # Prompt the user to select the warehouse by entering the index number
        try:
            selected_idx = input(
                "Enter the number of the warehouse you want to analyze data by (or 'A' for All Warehouses): ").strip().upper()
            if selected_idx == 'A':
                entity_id = None  # Means all warehouses
                entity_name = "All Warehouses"
            else:
                selected_idx = int(selected_idx)
                if selected_idx < 0 or selected_idx >= len(warehouses):
                    print(f"Invalid selection '{selected_idx}'")
                    return
                entity_id = warehouses[selected_idx]
                entity_name = entity_id
        except ValueError:
            print("Invalid input. Please enter a valid number.")
            return

        print(f"Selected warehouse: {entity_name}")

        # Filtering for warehouses is done after data_screening

    else:
        print("Invalid selection. Please enter 'C' for Client or 'W' for Warehouse.")
        return

    print("\nMain: Processing data...\n")

    (wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
     inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos,
     dispatched_inventory, inventario_sin_filtro) = data_processing(
        wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
        inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory,
        inventario_sin_filtro)

    # Debugging: Print processed data
    print("\nProcessed Data:\n")
    for name, df in zip([
        'wl_ingresos', 'rpshd_despachos', 'rpsdt_productos',
        'registro_ingresos', 'registro_salidas', 'inmovih_table',
        'saldo_inventory', 'supplier_info', 'ctcentro_table',
        'producto_modelos', 'dispatched_inventory', 'inventario_sin_filtro'
    ], [
        wl_ingresos, rpshd_despachos, rpsdt_productos,
        registro_ingresos, registro_salidas, inmovih_table,
        saldo_inventory, supplier_info, ctcentro_table,
        producto_modelos, dispatched_inventory, inventario_sin_filtro
    ]):
        print(f"{name} (After Processing):\n", df.head(), "\n")

    print("\nMain: Screening data...")

    # Filtrar las tablas por bodega
    (saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos_s, rpshd_despachos, wl_ingresos,
     inmovih_table, dispatched_inventory) = data_screening(saldo_inventory, registro_ingresos, registro_salidas,
                                                           rpsdt_productos, rpshd_despachos, wl_ingresos, inmovih_table,
                                                           dispatched_inventory)

    # Debugging: Print screened data
    print("\nScreened Data:\n")
    for name, df in zip([
        'saldo_inventory', 'registro_ingresos', 'registro_salidas', 'rpsdt_productos_s',
        'rpshd_despachos', 'wl_ingresos', 'inmovih_table', 'dispatched_inventory'
    ], [
        saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos_s,
        rpshd_despachos, wl_ingresos, inmovih_table, dispatched_inventory
    ]):
        print(f"{name} (After Screening):\n", df.head(), "\n")

    resumen_mensual_ingresos_clientes, resumen_mensual_ingresos_sd, resumen_mensual_ingresos_fact = (
        monthly_receptions_summary(registro_ingresos, supplier_info,
                                   inventario_sin_filtro, rpsdt_productos))

    # Handle unknown 'bodega' values after merging (ensures idcontacto_x is available)
    for df_name, df in zip(
            ['saldo_inventory', 'registro_ingresos', 'registro_salidas'],
            [saldo_inventory, registro_ingresos, registro_salidas]
    ):
        print(f"Processing 'handle_unknown_bodega' for {df_name}")
        df, eligible_rows, replaced_rows, remaining_rows = handle_unknown_bodega(df)

    # If warehouse analysis, filter DataFrames after data_screening
    if analysis_type == 'W' and entity_id is not None:
        # List of DataFrames to filter by warehouse
        dataframes_to_filter = [wl_ingresos, rpshd_despachos, rpsdt_productos,
                                registro_ingresos, registro_salidas, inmovih_table, saldo_inventory,
                                resumen_mensual_ingresos_sd, resumen_mensual_ingresos_fact, resumen_mensual_ingresos_clientes]

        """These two dataframes does not have 'bodega' column. [wl_ingresos, rpshd_despachos]"""

        # Filter DataFrames based on the selected warehouse
        filtered_dataframes = filter_dataframes_by_warehouse(dataframes_to_filter, entity_id)

        # Debugging: Print filtered data by warehouse
        print("\nFiltered Data by Warehouse:\n")
        for name, df in zip([
            'wl_ingresos', 'rpshd_despachos', 'rpsdt_productos',
            'registro_ingresos', 'registro_salidas', 'inmovih_table',
            'saldo_inventory', 'resumen_mensual_ingresos_sd', 'resumen_mensual_ingresos_fact'
        ], filtered_dataframes):
            print(f"{name} (Filtered by Warehouse):\n", df.head(), "\n")

        # Unpack filtered DataFrames
        (wl_ingresos, rpshd_despachos, rpsdt_productos,
         registro_ingresos, registro_salidas, inmovih_table, saldo_inventory, resumen_mensual_ingresos_sd,
         resumen_mensual_ingresos_fact,resumen_mensual_ingresos_clientes) = filtered_dataframes

    print("\nMain: Generating all reception data by warehouse and client...\n")

    # Debugging: Print monthly reception summaries
    print("\nMonthly Receptions Summary:\n")
    print("resumen_mensual_ingresos_clientes:\n", resumen_mensual_ingresos_clientes.head(), "\n")
    print("resumen_mensual_ingresos_sd:\n", resumen_mensual_ingresos_sd.head(), "\n")
    print("resumen_mensual_ingresos_fact:\n", resumen_mensual_ingresos_fact.head(), "\n")

    print("\nMain: Generating all dispatch data by warehouse and client...\n")

    resumen_mensual_despachos_clientes_grouped, merged_despachos_inventario, resumen_despachos_cliente_fact = (
        monthly_dispatch_summary(registro_salidas, dispatched_inventory, supplier_info))

    # Debugging: Print monthly dispatch summaries
    print("\nMonthly Dispatch Summary:\n")
    print("resumen_mensual_despachos_clientes_grouped:\n", resumen_mensual_despachos_clientes_grouped.head(), "\n")
    print("merged_despachos_inventario:\n", merged_despachos_inventario.head(), "\n")
    print("resumen_despachos_cliente_fact:\n", resumen_despachos_cliente_fact.head(), "\n")

    if not resumen_mensual_ingresos_clientes.empty and not resumen_mensual_despachos_clientes_grouped.empty:
        resumen_mensual_ingresos_bodega, resumen_mensual_despachos_bodega = group_by_month_bodega(
            resumen_mensual_ingresos_clientes, resumen_mensual_despachos_clientes_grouped, start_date, end_date)
    else:
        print("\nCannot proceed with grouping by Bodega due to lack of data.\n")

    saldo_inventory = insaldo_bode_comp(saldo_inventory)

    print("\nMain: Analysing actual Inventory...\n")

    if not saldo_inventory.empty and not supplier_info.empty:
        saldo_inv_cliente_fact = capacity_measured_in_cubic_meters(saldo_inventory, supplier_info)
        inventory_proportions_by_product(saldo_inventory, supplier_info)
        inventory_oldest_products(saldo_inventory, supplier_info)
    else:
        print("\nCannot proceed with inventory status calculations - Client currently has no "
              "product on any warehouse.\n")

    print("\nMain: Reconstructing data for billing...\n")

    if not saldo_inv_cliente_fact.empty:
        inflow_with_mode_historical, outflow_with_mode_historical, final_df = (
            billing_data_reconstruction(saldo_inv_cliente_fact, resumen_mensual_ingresos_fact,
                                        resumen_despachos_cliente_fact, start_date, end_date, registro_ingresos, supplier_info))
    else:
        print("\nCannot proceed with inventory status calculations - Client currently has no "
              "product on any warehouse.\n")

    print("\nMain: Reconstructing historic Inventory data and behavior...\n")

    inventory_over_time, inventory_ot_by_month = reconstruct_inventory_over_time(
        inflow_with_mode_historical,
        outflow_with_mode_historical, start_date=None, end_date=None
    )

    selected_month_data = filtering_historic_insaldo(inventory_over_time, start_date, end_date)

    print("\nMain: Calculating relevant KPIs for analysis...\n")

    kpi_calculation(inventory_over_time, inventory_ot_by_month, start_date, end_date)


if __name__ == "__main__":
    main()
