from utils.data_utils import filter_dataframes_by_idcontacto

def filter_by_client(dataframes, supplier_info):
    """
    Filter dataframes for a specific client.

    Args:
        dataframes (list of pd.DataFrame): The dataframes to filter.
        supplier_info (pd.DataFrame): Supplier information DataFrame.

    Returns:
        tuple: (entity_id, entity_name, filtered_dataframes)
    """
    # Display the list of clients
    unique_clients = supplier_info[['idcontacto', 'descrip']].drop_duplicates().reset_index(drop=True)
    print("List of clients:")
    for idx, row in unique_clients.iterrows():
        print(f"{idx}: {row['descrip']} ({row['idcontacto']})")

    # Prompt the user to select the client
    try:
        selected_idx = int(input("Enter the number of the client you want to analyze data by: "))
        if selected_idx < 0 or selected_idx >= len(unique_clients):
            raise ValueError(f"Invalid selection '{selected_idx}'")
    except ValueError as e:
        print(f"Error: {e}")
        return None, None, None

    # Identify selected client
    selected_entity = unique_clients.iloc[selected_idx]
    entity_id = selected_entity['idcontacto']
    entity_name = selected_entity['descrip']
    print(f"Selected client: {entity_name} (idcontacto: {entity_id})")

    # Filter dataframes by client
    filtered_dataframes = filter_dataframes_by_idcontacto(dataframes, entity_id)

    return entity_id, entity_name, filtered_dataframes