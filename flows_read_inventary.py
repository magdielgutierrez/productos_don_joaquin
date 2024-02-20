from prefect import flow, task
from prefect.filesystems import SMB
from io import StringIO
import pandas as pd

# smb_block = SMB.load("don-joaquin-inventario")
# data=smb_block.read_path('/INVENTARIO PLANTA 2024/PLANILLA DE LECHE 2024.xlsx')

# xls = pd.ExcelFile(data.decode('ISO-8859-1'))
# sheets = xls.sheet_names

# results = {}
# for sheet in sheets:
#     results[sheet] = xls.parse(sheet)
    
# xls.close()
    

@task
def get_file_inventory(smb_block):
    
    data=smb_block.read_path('/INVENTARIO PLANTA 2024/PLANILLA DE LECHE 2024.xlsx')

    xls = pd.ExcelFile(data)
    sheets = xls.sheet_names
  
    results = {}
    for sheet in sheets:
        results[sheet] = xls.parse(sheet)
        
    xls.close()
    
    return results,sheets


@flow(log_prints=True)
def flows_read_inventory():
    smb_block = SMB.load("don-joaquin-inventario")
    results, sheets=get_file_inventory(smb_block)

    print('\n',sheets)

if __name__ == "__main__":
    flows_read_inventory.serve(name="productos_don_joaquin_produccion")
