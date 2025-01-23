from data.data_load import load_data
from analysis_focus.client_focus import filter_by_client
from data_processing.data_processing import data_processing
from data_processing.data_screening import data_screening
from data_processing.monthly_summary import monthly_receptions_summary, monthly_dispatch_summary
from utils.kpi_calculations import kpi_calculation
from utils.inventory_proportions import inventory_proportions_by_product
from utils.actual_inventory import inventory_oldest_products
from data_processing.billing_reconstruction import billing_data_reconstruction
from data_processing.inventory_behavior_reconstruction import reconstruct_inventory_over_time
from utils.date_utils import get_date_range
from utils.grouping_functions import  group_by_month_bodega
from utils.insaldo_complement import insaldo_bode_comp
from utils.actual_inventory import capacity_measured_in_cubic_meters
import pandas as pd

# Specify default dates (optional)
default_start = '01/12/2024'
default_end = '31/12/2024'

# Get the date range from the user
start_date, end_date = get_date_range(default_start, default_end)

print(f"Analysis will run for the range: {start_date.date()} to {end_date.date()}")

# Step 1: Load Raw Data
print("Loading data...")
(wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
 inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos,
 dispatched_inventory, inventario_sin_filtro) = load_data()

# Step 2: Client Focus Analysis
print("Filtering data by client...")
dataframes_to_filter = [
    wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
    inmovih_table, saldo_inventory, dispatched_inventory, inventario_sin_filtro
]
entity_id, entity_name, filtered_dataframes = filter_by_client(dataframes_to_filter, supplier_info)

if not filtered_dataframes:
    print("No data available for the selected client.")
    exit()

# Unpack filtered dataframes
(wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
 inmovih_table, saldo_inventory, dispatched_inventory, inventario_sin_filtro) = filtered_dataframes

# Step 3: Data Processing
print("Processing data...")
(wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
 inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos,
 dispatched_inventory, inventario_sin_filtro) = data_processing(
    wl_ingresos, rpshd_despachos, rpsdt_productos, registro_ingresos, registro_salidas,
    inmovih_table, saldo_inventory, supplier_info, ctcentro_table, producto_modelos, dispatched_inventory,
    inventario_sin_filtro
)

# Step 4: Data Screening
print("Screening data...")
(saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos, rpshd_despachos, wl_ingresos,
 inmovih_table, dispatched_inventory) = data_screening(
    saldo_inventory, registro_ingresos, registro_salidas, rpsdt_productos, rpshd_despachos,
    wl_ingresos, inmovih_table, dispatched_inventory
)

pd.set_option(
    "display.max_rows", 100,
    "display.max_columns", None,
    "display.expand_frame_repr", False
)

# Step 5: Monthly Summaries and data prep
print("Generating monthly summaries...")
resumen_mensual_ingresos_clientes, resumen_mensual_ingresos_sd, resumen_mensual_ingresos_fact = monthly_receptions_summary(
    registro_ingresos, supplier_info, inventario_sin_filtro, rpsdt_productos
)
resumen_mensual_despachos_clientes_grouped, merged_despachos_inventario, resumen_despachos_cliente_fact = monthly_dispatch_summary(
    registro_salidas, dispatched_inventory, supplier_info
)

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

# Step 6: Billing Data Reconstruction
print("Reconstructing billing data...")
if not saldo_inv_cliente_fact.empty:
    inflow_with_mode_historical, outflow_with_mode_historical, final_df = billing_data_reconstruction(
        saldo_inv_cliente_fact, resumen_mensual_ingresos_fact, resumen_despachos_cliente_fact,
        start_date, end_date, registro_ingresos, supplier_info
    )
else:
    print("No inventory data available for billing reconstruction.")
    inflow_with_mode_historical, outflow_with_mode_historical = None, None

# Step 7: Behavior Over Time
if inflow_with_mode_historical is not None and outflow_with_mode_historical is not None:
    print("Reconstructing inventory behavior over time...")
    inventory_over_time, inventory_ot_by_month = reconstruct_inventory_over_time(
        inflow_with_mode_historical, outflow_with_mode_historical
    )
else:
    print("Skipping inventory behavior reconstruction due to missing inflow/outflow data.")
    inventory_over_time, inventory_ot_by_month = None, None

# Step 8: Inventory Analysis
print("Calculating KPIs and analyzing inventory...")
if inventory_over_time is not None and not inventory_over_time.empty:
    print("Calculating KPIs...")
    kpis = kpi_calculation(inventory_over_time, inventory_ot_by_month, start_date, end_date)
else:
    print("Skipping KPI calculations due to missing inventory data.")