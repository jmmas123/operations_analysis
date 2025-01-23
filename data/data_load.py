from rich.progress import Progress
from utils.path_utils import get_base_path, get_base_output_path
import pandas as pd
import os
import time

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

        # Save the final DataFrame to CSV
        output_path = os.path.join(get_base_output_path(), 'registro_ingresos_load_data.csv')
        registro_ingresos.to_csv(output_path, index=False)

    print("\nData loaded correctly.\n")

    return (
        wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas, inmovih_table,
        saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory, inventario_sin_filtro
    )
