from utils.data_utils import filter_dataframes_by_warehouse

def filter_by_warehouse(dataframes):
    """
    Filter dataframes for a specific warehouse.

    Args:
        dataframes (list of pd.DataFrame): The dataframes to filter.

    Returns:
        tuple: (entity_id, entity_name, filtered_dataframes)
    """
    # Display the list of warehouses
    warehouses = ["BODA", "BODC", "BODE", "BODG", "BODJ", "OPL", "INCOHERENT VALUES", "DESCONOCIDO", "INTEMPERIE", "PISO"]
    print("List of warehouses:")
    for idx, warehouse in enumerate(warehouses):
        print(f"{idx}: {warehouse}")

    # Prompt the user to select the warehouse
    try:
        selected_idx = input("Enter the number of the warehouse you want to analyze data by (or 'A' for All Warehouses): ").strip().upper()
        if selected_idx == 'A':
            return None, "All Warehouses", dataframes
        selected_idx = int(selected_idx)
        if selected_idx < 0 or selected_idx >= len(warehouses):
            raise ValueError(f"Invalid selection '{selected_idx}'")
    except ValueError as e:
        print(f"Error: {e}")
        return None, None, None

    # Identify selected warehouse
    entity_id = warehouses[selected_idx]
    entity_name = entity_id
    print(f"Selected warehouse: {entity_name}")

    # Filter dataframes by warehouse
    filtered_dataframes = filter_dataframes_by_warehouse(dataframes, entity_id)

    return entity_id, entity_name, filtered_dataframes