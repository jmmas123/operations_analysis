import os
import pandas as pd
import numpy as np
from dash import Dash
# from scipy.interpolate import make_interp_spline
import tkinter as tk
from tkinter import filedialog
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils.dataframe import dataframe_to_rows
# import matplotlib.pyplot as plt
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Font, PatternFill, Border, Side


from operational_data import load_data, data_processing, data_screening, insaldo_bode_comp, \
    monthly_receptions_summary, monthly_dispatch_summary, group_by_month_bodega, capacity_measured_in_cubic_meters, \
    billing_data_reconstruction, inventory_proportions_by_product, inventory_oldest_products, \
    reconstruct_inventory_over_time, filtering_historic_insaldo, filter_dataframes_by_idcontacto, parse_date

from overtime_data import load_data, data_normalization, cost_calculator, adjust_overlapping_costs, group_operations, \
    income_calculator




# Determinar el área de análisis
def get_base_path():
    if os.name == 'nt':  # Windows
        return r'\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\Facturacion\Tarifas\\'
    else:  # MacOS (or others)
        return '/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Facturacion/Tarifas/'


def get_base_output_path():
    if os.name == 'nt':  # Windows
        # obase_path = r'C:\Users\melanie\Downloads'
        obase_path = r'C:\Users\josemaria\Downloads'

    else:  # MacOS (or others)
        obase_path = r'/Users/j.m./Downloads'
    return obase_path

def tarifs_df_loading():

    base_path = get_base_path()  # Get the correct base path based on the OS

    # Full file path
    file_path = os.path.join(base_path, 'TARIFAS DE CLIENTES 2024.xlsx')

    # Load the Excel file as a DataFrame
    tarifs_df = pd.read_excel(file_path, engine='openpyxl', dtype=str)

    # Print DataFrame for debugging
    print("Tarifs df:\n", tarifs_df)

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

    tarfis_df = tarifs_df_loading()

    # Convert 'descrip' and 'idcontacto' to string and strip whitespaces
    supplier_info['descrip'] = supplier_info['descrip'].astype(str).str.strip()
    supplier_info['idcontacto'] = supplier_info['idcontacto'].astype(str).str.strip()

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

    selected_client = unique_clients.iloc[selected_idx]
    idcontacto = selected_client['idcontacto']
    print(f"Selected client: {selected_client['descrip']} (idcontacto: {idcontacto})")

    idcontacto = selected_client['idcontacto']
    print(f"Selected client: {selected_client['descrip']} (idcontacto: {idcontacto})")

    # List of DataFrames to filter
    dataframes_to_filter = [wl_ingresos, rpshd_despachos, rpsdt_productos,
                            registro_ingresos, registro_salidas, inmovih_table, saldo_inventory,
                            dispatched_inventory, inventario_sin_filtro]

    # Filter DataFrames
    filtered_dataframes = filter_dataframes_by_idcontacto(dataframes_to_filter, idcontacto)

    # Unpack filtered DataFrames
    (wl_ingresos, rpshd_despachos, rpsdt_productos,
     registro_ingresos, registro_salidas, inmovih_table, saldo_inventory,
     dispatched_inventory, inventario_sin_filtro) = filtered_dataframes

    print("\nMain: Processing data...\n")

    (wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
     inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos,
     dispatched_inventory, inventario_sin_filtro) = data_processing(
        wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
        inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory,
        inventario_sin_filtro)

    print("\nMain: Screening data...")

    # Filtrar las tablas por bodega
    (saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos_s, rpshd_despachos, wl_ingresos,
     inmovih_table, dispatched_inventory) = data_screening(saldo_inventory, registro_ingresos, registro_salidas,
                                                           rpsdt_productos, rpshd_despachos, wl_ingresos, inmovih_table,
                                                           dispatched_inventory)

    print("\nMain: Generating all reception data by warehouse and client...\n")

    resumen_mensual_ingresos_clientes, resumen_mensual_ingresos_sd, resumen_mensual_ingresos_fact = (
        monthly_receptions_summary(registro_ingresos, supplier_info,
                                   inventario_sin_filtro, rpsdt_productos))

    print("\nMain: Generating all dispatch data by warehouse and client...\n")

    resumen_mensual_despachos_clientes_grouped, merged_despachos_inventario, resumen_despachos_cliente_fact = (
        monthly_dispatch_summary(registro_salidas, dispatched_inventory, supplier_info))

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
                                        resumen_despachos_cliente_fact, start_date, end_date, registro_ingresos))
    else:
        print("\nCannot proceed with inventory status calculations - Client currently has no "
              "product on any warehouse.\n")

    print("\nMain: Reconstructing historic Inventory data and behavior...\n")


    inventory_over_time, inventory_ot_by_month = reconstruct_inventory_over_time(
        inflow_with_mode_historical,
        outflow_with_mode_historical, start_date=None, end_date=None
    )

    selected_month_data = filtering_historic_insaldo(inventory_over_time, start_date, end_date)

    print("\nMain: Loading tariff data...\n")

    tarfis_df = tarifs_df_loading()

    print("\nMain: Merging Billing with operational data...")



if __name__ == "__main__":
    main()