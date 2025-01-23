import datetime
import pandas as pd
import numpy as np


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

def clip_near_zero(df, columns=None, epsilon=1e-6):
    """
    For each column in `columns`, set the value to 0 if abs(value) < epsilon.
    If columns is None, applies to all float columns in df.
    """
    if columns is None:
        # Identify float columns automatically
        columns = df.select_dtypes(include=[np.number]).columns

    for col in columns:
        df[col] = df[col].apply(lambda x: 0 if abs(x) < epsilon else x)

    return df


