import pandas as pd
from datetime import datetime

def parse_date(date_str):
    """
    Parse a date string into a datetime object.

    Args:
        date_str (str): Date string in formats like 'dd-mm-yyyy', 'dd/mm/yyyy', etc.

    Returns:
        datetime: Parsed datetime object.
    """
    for fmt in ('%d-%m-%Y', '%d-%m-%y', '%d/%m/%Y', '%d/%m/%y'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError("Invalid date format. Please enter dates in dd/mm/yy or dd-mm-yy format.")

def get_date_range(default_start=None, default_end=None):
    """
    Get a date range from user input with optional defaults.

    Args:
        default_start (str): Default start date as a string (e.g., '01/01/2024').
        default_end (str): Default end date as a string (e.g., '31/12/2024').

    Returns:
        tuple: Start and end dates as pandas.Timestamp objects.
    """
    # Prompt for dates with default values
    start_date_str = input(f"Enter the start date of analysis (default {default_start}): ") or default_start
    end_date_str = input(f"Enter the end date of analysis (default {default_end}): ") or default_end

    # Convert to pandas.Timestamp
    try:
        start_date = pd.Timestamp(parse_date(start_date_str))
        end_date = pd.Timestamp(parse_date(end_date_str))

        if start_date > end_date:
            raise ValueError("Start date must be before or equal to the end date.")

        return start_date, end_date
    except ValueError as e:
        print(f"Invalid date input: {e}")
        exit()

def validate_date_range(start_date, end_date):
    """
    Validate that start date is before or equal to end date.

    Args:
        start_date (datetime or str): Start date.
        end_date (datetime or str): End date.

    Returns:
        bool: True if the range is valid, otherwise raises ValueError.
    """
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        raise ValueError("Start date must be before or equal to the end date.")
    return True