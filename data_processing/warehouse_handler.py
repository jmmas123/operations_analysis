import pandas as pd
import os


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

    # Create a filtered DataFrame excluding 'DESCONOCIDO'
    filtered_bodegas = merged_ingresos_inventario[merged_ingresos_inventario['bodega'] != 'DESCONOCIDO']

    # Find replacement bodega for each idcontacto_x where there is exactly one unique bodega
    replacement_bodega = filtered_bodegas.groupby('idcontacto').filter(lambda x: len(x['bodega'].unique()) == 1)
    replacement_bodega = replacement_bodega.groupby('idcontacto')['bodega'].first()

    # Create mask for rows where bodega is 'DESCONOCIDO' and valid replacement exists
    mask = (merged_ingresos_inventario['bodega'] == 'DESCONOCIDO') & \
           merged_ingresos_inventario['idcontacto'].isin(replacement_bodega.index)

    # Apply the replacement
    merged_ingresos_inventario.loc[mask, 'bodega'] = merged_ingresos_inventario.loc[mask, 'idcontacto'].map(
        replacement_bodega)


    return merged_ingresos_inventario
