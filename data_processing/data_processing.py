from rich.progress import Progress
import pandas as pd
import time

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
            # else:
            #     print(f"Warning: '{col}' column contains non-datetime values")

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

    print("\nData Processing completed successfully.\n")
    return (wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
            inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory,
            inventario_sin_filtro)