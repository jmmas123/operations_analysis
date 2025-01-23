import pandas as pd
import os
from utils import get_base_output_path


def group_by_month_bodega(resumen_mensual_ingresos_clientes, resumen_mensual_despachos_clientes, start_date,
                          end_date):
    print("\n*** Final monthly inflow and outflow dataframes by warehouse ***\n")

    # Ensure 'fecha_x' is in datetime format
    resumen_mensual_ingresos_clientes['fecha'] = pd.to_datetime(resumen_mensual_ingresos_clientes['fecha_x'],
                                                                errors='coerce')
    resumen_mensual_despachos_clientes['fecha'] = pd.to_datetime(resumen_mensual_despachos_clientes['fecha_x'],
                                                                 errors='coerce')

    # # Save the final DataFrame to CSV
    # output_path = os.path.join(get_base_output_path(), 'resumen_historico_ingresos_clientes.csv')
    # resumen_mensual_ingresos_clientes.to_csv(output_path, index=False)
    # output_path = os.path.join(get_base_output_path(), 'resumen_historico_despachos_clientes.csv')
    # resumen_mensual_despachos_clientes.to_csv(output_path, index=False)

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

    output_path = os.path.join(get_base_output_path(), 'resumen_mensual_despachos_clientes.csv')
    resumen_mensual_despachos_clientes.to_csv(output_path, index=False)

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
