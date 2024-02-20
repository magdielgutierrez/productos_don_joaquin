from prefect import flow, task
from prefect.filesystems import SMB
from io import StringIO
import pandas as pd
import re

def _textDateComplete(txt_mes,txt_days,year):
    
    dias = [int(valor) for valor in txt_days]
    start=dias
    lstMonths=txt_mes.split(',')

    fechas=[]
    for month in lstMonths:
        monthName = re.search(r'([A-Z]+)', month)
        numMinDays= re.search(r'([0-9]+)', month)
        
        monthName = monthName.group(1).replace('.','')
        numMinDays = int(numMinDays.group(1))
        meses_mapping = {'ENERO': '1', 'FEBRERO': '2', 'MARZO': '3', 'ABRIL': '4', 'MAYO': '5', 'JUNIO': '6',
                            'JULIO': '7', 'AGOSTO': '8', 'SEPTIEMBRE': '9', 'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12',
                            'ENE': '1','EN': '1', 'ENER': '1','FEB': '2', 'MAR': '3','MARZ': '3', 'ABR': '4', 'MAY': '5', 'JUN': '6',
                            'JUL': '7', 'AGO': '8', 'SEP': '9', 'OCT': '10', 'NOV': '11', 'DIC': '12'}

        numero_mes = meses_mapping.get(monthName.upper())
                   
        dates = [ (str(dia) + '/' + numero_mes + '/' + year ) for dia in dias if dia >= numMinDays ] 

        dias = [dia for dia in dias if dia < numMinDays]

        fechas=fechas+dates
    
    resultado = [elemento for elemento in start if elemento  in dias]        
    return  resultado+fechas


def _clasificarProveedor(nombre, precio):
    if 'trans' in str(nombre).lower() or precio < 0.10:
        return 'Transporte'
    else:
        return 'Productor'


def _quitarLetras(valor):
    return re.sub(r'\D', '', valor)


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

@task
def _formatSheets(rDataOne,key):
    txt_year='2024'
    if rDataOne.columns.values.tolist()[0] != 'LACTEOS DON JOAQUIN ':
        
        rDataOne.loc[1,'REGISTRO DE COMPRA DE LECHE '] = "tmpProveedor"
        rDataOne.loc[1,'Unnamed: 1'] = "tmpCosto"
        rDataOne = rDataOne.fillna(0)
            
        # actulizar encabezados de df
        df=pd.concat([rDataOne.iloc[2], rDataOne.iloc[1]], axis=1)
        df = df.fillna('0')
        df[1] = df[1].astype(str)
        df[2] = df[2].astype(str)
        df['col']=df[2]+df[1]
            
            
        rDataOne.columns = df['col'].tolist()
        rDataOne = rDataOne.drop(rDataOne.index[1])
        rDataOne=rDataOne[rDataOne['0tmpCosto'] != 0]
        rDataOne=rDataOne.drop(['0LITROS','0TOTAL'], axis=1)
        columnas_a_eliminar = rDataOne.columns[(rDataOne == 0).all()]
        rDataOne = rDataOne.drop(columns=columnas_a_eliminar)
                
         # fechas por dias
        nameColumnsOld=rDataOne.columns.values.tolist()
        txt_days = [_quitarLetras(valor) for valor in nameColumnsOld[2:]]
        

        addCols=_textDateComplete(key,txt_days,txt_year)
        resultado = [valor for valor in addCols if isinstance(valor, str)]
        nameColumnsNew= ['Proveedor', 'PrecioUnit'] + resultado
                    
        for colOld, colNew in zip(nameColumnsOld, nameColumnsNew): 
            rDataOne=rDataOne.rename(columns={colOld:colNew})

        rDataOne['Subsidio']=0
        rDataOne=rDataOne[['Proveedor', 'PrecioUnit','Subsidio'] + resultado]
        return rDataOne
        
    else:
                
        # formatear columnas de archivo 
        rDataOne.loc[2,'LACTEOS DON JOAQUIN '] = "tmpProveedor"
        rDataOne.loc[2,'Unnamed: 1'] = "tmpCosto"
        rDataOne = rDataOne.fillna(0)
            
        # actulizar encabezados de df
        df=pd.concat([rDataOne.iloc[3], rDataOne.iloc[2]], axis=1)
        df = df.fillna('0')
        df[2] = df[2].astype(str)
        df[3] = df[3].astype(str)
        df['col']=df[3]+df[2]
            
            
        rDataOne.columns = df['col'].tolist()
        rDataOne = rDataOne.drop(rDataOne.index[2])
        rDataOne=rDataOne[rDataOne['0tmpCosto'] != 0]
        rDataOne=rDataOne.drop(['0LITROS' ,'0T SEM.' ,'0T.SUB', '0TOTAL'], axis=1)
        columnas_a_eliminar = rDataOne.columns[(rDataOne == 0).all()]
        rDataOne = rDataOne.drop(columns=columnas_a_eliminar)
                
        # fechas por dias
        nameColumnsOld=rDataOne.columns.values.tolist()
        txt_days = [_quitarLetras(valor) for valor in nameColumnsOld[2:]]
            
        addCols=_textDateComplete(key,txt_days,txt_year)
        resultado = [valor for valor in addCols if isinstance(valor, str)]
        nameColumnsNew= ['Proveedor', 'PrecioUnit','Subsidio'] + resultado
                    
        for colOld, colNew in zip(nameColumnsOld, nameColumnsNew): 
            rDataOne=rDataOne.rename(columns={colOld:colNew})
                
        rDataOne=rDataOne[['Proveedor', 'PrecioUnit','Subsidio'] + resultado]
        
        return rDataOne

@flow(log_prints=True)
def flows_read_inventory():
    smb_block = SMB.load("don-joaquin-inventario")
    results, sheets=get_file_inventory(smb_block)

    print('\n')

    for key, rawData in results.items():
        
        print(f"\t Nombre de la Hoja que se esta extrayendo la data {key}")
               
        rawData= _formatSheets(rawData,key)
        print(rawData)
        smb_block.write_path(path='filename.txt', content=rawData.encode('utf-8'))
        break

if __name__ == "__main__":
    flows_read_inventory.from_source(
        source='https://github.com/magdielgutierrez/productos_don_joaquin.git',
        entrypoint="don_joaquin_inventory.py:flows_read_inventory",
    ).deploy(
        name="don_joaquin_produccion",
        work_pool_name="don-joaquin-lacteos",
        cron="0 13 * * *",
    )
