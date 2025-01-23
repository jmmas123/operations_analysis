import os
import socket
import pandas as pd
from utils import get_base_output_path


def insaldo_bode_comp(saldo_inventory):
    # Leer los datos especificando los tipos de datos
    if os.name == 'nt':  # Windows
        # Network path for Windows
        inmodelo_clasificacion = pd.read_excel('\\192.168.10.18\gem\006 MORIBUS\ANALISIS y PROYECTOS\varios\modelos_clasificacion.xlsx')
    else:  # macOS or others
        hostname = socket.gethostname()
        if 'JM-MS.local' in hostname:  # For Mac Studio
            inmodelo_clasificacion = pd.read_excel(r'/Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')
        elif 'MacBook-Pro.local' in hostname:  # For MacBook Pro
            inmodelo_clasificacion = pd.read_excel(r'/Users/j.m./Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/varios/modelos_clasificacion.xlsx')
        else:
            raise ValueError(f"Unknown hostname: {hostname}. Unable to determine file path.")


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

