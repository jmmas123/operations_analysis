# utils/path_utils.py
import socket
import os

def get_clean_hostname():
    hostname = socket.gethostname()
    if hostname.endswith('.local'):
        hostname = hostname.replace('.local', '')
    return hostname

def get_base_path():
    if os.name == 'nt':
        return r'C:\Users\josemaria\Downloads'
    else:
        hostname = get_clean_hostname()
        if hostname == 'MacBook-Pro':
            return '/Users/j.m./Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Tablas/'
        elif hostname == 'JM-MS':
            return '/Users/jm/Library/Mobile Documents/com~apple~CloudDocs/GM/MOBU - OPL/Tablas/'
        return None

def get_base_output_path():
    if os.name == 'nt':
        return r'C:\Users\josemaria\Downloads'
    else:
        hostname = get_clean_hostname()
        if hostname == 'MacBook-Pro':
            return '/Users/j.m./Downloads'
        elif hostname == 'JM-MS':
            return '/Users/jm/Downloads'
        return None