import pandas as pd
import os
import shutil
from functools import reduce

# =============================================================================
# VARIABLES
# =============================================================================
os.chdir('/Users/luis.salamo/Documents/github enterprise/python-training/lsf')
DIR_PARENT = os.getcwd()
DIR_EXPORT = 'export' 

# =============================================================================
# WORKDERS
# =============================================================================

df_workers = pd.read_csv(DIR_PARENT + "/lsf_workers.csv", header=None)  
df_workers.rename(
    columns={
        0: 'NOMBRE',
        1: 'LOCALIDAD',
        2: 'TELEFONO',
        3: 'H/S',
        4: 'TIPO',
        5: 'TIPO CONTRATO',
        6: 'CATEGORIA',
        7: 'ANTIGUEDAD',
        8: 'CONVENIO',
        9: 'VEHICULO EMPRESA',        
        10: 'BAJA LABORAL',
        11: 'TOTAL DEVENGOS',
        12: 'COSTE S.S.',
        13: 'EMPRESA',
        14: 'PLUSES',
        15: 'TAREAS GENERALES'
    },
    inplace=True
)

df_workers['CODIGO'] = df_workers.index + 1
df_workers.set_index('CODIGO', inplace=True)
df_workers = df_workers[['NOMBRE', 'LOCALIDAD', 'TELEFONO', 'H/S', 'TIPO', 'TIPO CONTRATO', 'CATEGORIA', 'ANTIGUEDAD', 'CONVENIO', 'VEHICULO EMPRESA', 'BAJA LABORAL', 'TOTAL DEVENGOS', 'COSTE S.S.', 'EMPRESA', 'PLUSES', 'TAREAS GENERALES']]

# =============================================================================
# BILLING
# =============================================================================

# get data
def get_billing_data(file):
    df = pd.read_csv(DIR_PARENT + '/' + file, header=None)  
    df.rename(
        columns={
            0: 'CUENTA',
            1: 'NOMBRE',
            2: 'TIPO CLIENTE',
            3: 'LOCALIDAD',
            4: 'CLIENTE PUBLICO',
            5: 'CUOTA MENSUAL',
            6: 'FACTURACION ANUAL',
            7: 'CODIGO TRABAJADOR',
            8: 'H/S',
            9: 'H/A',
            10: 'KM/S',
            11: 'TAREAS EXTRAS'
        },
        inplace=True
    )
    # df['CODIGO'] = 'CLIENT_' + (df.index + 1).astype(str)
    df['CODIGO TRABAJADOR'] = df['CODIGO TRABAJADOR'].astype(str)
    df['NUMERO TRABAJADORES'] = df['CODIGO TRABAJADOR'].apply(lambda x: 0 if not x else len(x.split(','))) 
    df['H/S BILLING'] = df['H/S'] / df['NUMERO TRABAJADORES'] 
    df['COSTE EXTRA/SEMANA'] = df['KM/S'] * 0.2    
    df['CODIGO'] = df.index + 1
    df.set_index('CODIGO', inplace=True)
    return df

df_lsf_billing = get_billing_data('lsf_billing.csv')
df_lsac_billing = get_billing_data('lsac_billing.csv')
df_gi_billing = get_billing_data('gi_billing.csv')

# =============================================================================
# QA BILLING WORKER H/S
# =============================================================================

def get_billing_data_split_worker(df,target_column,separator):
    row_accumulator = []
    def splitListToRows(row, separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)
    df.apply(splitListToRows, axis=1, args = (separator, ))
    new_df = pd.DataFrame(row_accumulator)
    new_df = new_df[new_df['H/S'] > 0]
    return new_df

df_lsf_billing_qa = get_billing_data_split_worker(df_lsf_billing,'CODIGO TRABAJADOR',',')
df_lsac_billing_qa = get_billing_data_split_worker(df_lsac_billing,'CODIGO TRABAJADOR',',')
df_gi_billing_qa = get_billing_data_split_worker(df_gi_billing,'CODIGO TRABAJADOR',',')

def get_billing_worker(df1, df2, df3):  
    df1 = df1.groupby(by='CODIGO TRABAJADOR')['H/S BILLING'].sum().to_frame()
    df1.rename(columns={'H/S BILLING': 'H/S LSF'},inplace=True)
    df1.index = df1.index.astype('int64')
    df2 = df2.groupby(by='CODIGO TRABAJADOR')['H/S BILLING'].sum().to_frame()
    df2.rename(columns={'H/S BILLING': 'H/S LSAC'},inplace=True)
    df2.index = df2.index.astype('int64')
    df3 = df3.groupby(by='CODIGO TRABAJADOR')['H/S BILLING'].sum().to_frame()
    df3.rename(columns={'H/S BILLING': 'H/S GI'},inplace=True)
    df3.index = df3.index.astype('int64')
    
    dfs = [df1, df2, df3]
    df = reduce(lambda left,right: pd.merge(left, right, how='outer', on='CODIGO TRABAJADOR'), dfs)
    df["H/S BILLING"] = df.sum(axis=1)
    return df

df = get_billing_worker(df_lsf_billing_qa, df_lsac_billing_qa, df_gi_billing_qa)
df = pd.merge(df_workers, df, left_index=True, right_index=True, how='left')
df['H/S GAP'] = df['H/S BILLING'] - df['H/S']  
df = df[['NOMBRE', 'TIPO', 'BAJA LABORAL', 'H/S', 'H/S LSF', 'H/S LSAC', 'H/S GI', 'H/S BILLING', 'H/S GAP']]
df = df.sort_values(by=['H/S GAP'], ascending=False)

# query hours by worker id
df = pd.concat([df_lsf_billing, df_lsac_billing, df_gi_billing]) 
df = df[['NOMBRE', 'EMPRESA', 'CODIGO TRABAJADOR', 'H/S', 'H/A']]
worker = '27'
df_worker = df[
    (df['CODIGO TRABAJADOR'] == worker) |
    (df['CODIGO TRABAJADOR'].str.endswith(',' + worker)) |
    (df['CODIGO TRABAJADOR'].str.startswith(worker + ',')) |
    (df['CODIGO TRABAJADOR'].str.contains(',' + worker + ','))
]

# =============================================================================
# QA BILLING WORKER - CUOTA MENSUAL / (H/S * 4)
# =============================================================================

def get_price_hour_month(df):
    df = df.copy()
    df['CUOTA MENSUAL'] = df['CUOTA MENSUAL'].astype(str).str.replace(',', '').astype('float64')
    df['HORAS SEMANALES'] = df['H/S']
    df['HORAS MENSUALES'] = df['H/S'] * 4
    df['PRECIO HORA'] = df['CUOTA MENSUAL'] / df['HORAS MENSUALES']
    df = df[df['H/S'] > 0]
    df = df[['NOMBRE', 'CUOTA MENSUAL', 'HORAS SEMANALES', 'HORAS MENSUALES', 'PRECIO HORA']]
    df = df.sort_values(by=['PRECIO HORA'], ascending=False)
    return df

df = get_price_hour_month(df_lsf_billing)
df = get_price_hour_month(df_lsac_billing)
df = get_price_hour_month(df_gi_billing)

def get_price_hour_year(df):
    df = df.copy()
    df['FACTURACION ANUAL'] = df['FACTURACION ANUAL'].astype(str).str.replace(',', '').astype('float64')
    df['H/A'] = df['H/A'].astype(str).str.replace(',', '.').astype('float64')
    df['HORAS ANUALES'] = df['H/A']
    df['PRECIO HORA'] = df['FACTURACION ANUAL'] / df['HORAS ANUALES']
    df = df[df['H/A'] > 0]
    df = df[['NOMBRE', 'FACTURACION ANUAL', 'HORAS ANUALES', 'PRECIO HORA']]
    df = df.sort_values(by=['PRECIO HORA'], ascending=False)
    return df

df = get_price_hour_year(df_lsf_billing)
df = get_price_hour_year(df_lsac_billing)
df = get_price_hour_year(df_gi_billing)

# =============================================================================
# QA TIPO EMPRESA
# =============================================================================

df = df_lsf_billing
df['TIPO CLIENTE'].unique()
df = df_lsac_billing
df['TIPO CLIENTE'].unique()
df = df_gi_billing
df['TIPO CLIENTE'].unique()

# =============================================================================
#   RESULT
# =============================================================================

result = { 
    'total_workers': df_workers.shape[0], 
    'df_workers': df_workers,
    # 'df_billing_worker': df_billing_worker
}

a = df_lsf_billing['FACTURACION ANUAL']
a.sum()
df_lsf_billing.groupby['FACTURACION ANUAL'].sum(axis = 1)

# =============================================================================
#   EXPORT CSV
# =============================================================================

dir = os.path.join(DIR_PARENT, DIR_EXPORT)
if os.path.isdir(dir):
    shutil.rmtree(dir)
os.makedirs(dir)

df_workers.to_csv(dir + "/data_workers.csv")
# df_workers_maintenance.to_csv(dir + "/data_workers_maintenance.csv")
# df_workers_mobile.to_csv(dir + "/data_workers_mobile.csv")
# df_workers_management.to_csv(dir + "/data_workers_management.csv")

df = df_lsf_billing[['CUENTA', 'CODIGO', 'NOMBRE', 'TIPO CLIENTE', 'LOCALIDAD', 'CLIENTE PUBLICO', 'CUOTA MENSUAL', 'FACTURACION ANUAL', 'H/S', 'H/A', 'H/S BILLING', 'CODIGO TRABAJADOR', 'NUMERO TRABAJADORES', 'TAREAS EXTRAS', 'KM/S', 'COSTE EXTRA/SEMANA']]
df.to_csv(dir + "/data_lsf_billing.csv")
df = df_lsac_billing[['CUENTA', 'CODIGO', 'NOMBRE', 'TIPO CLIENTE', 'LOCALIDAD', 'CLIENTE PUBLICO', 'CUOTA MENSUAL', 'FACTURACION ANUAL', 'H/S', 'H/A', 'H/S BILLING', 'CODIGO TRABAJADOR', 'NUMERO TRABAJADORES', 'TAREAS EXTRAS', 'KM/S', 'COSTE EXTRA/SEMANA']]
df.to_csv(dir + "/data_lsac_billing.csv")
df = df_gi_billing[['CUENTA', 'CODIGO', 'NOMBRE', 'TIPO CLIENTE', 'LOCALIDAD', 'CLIENTE PUBLICO', 'CUOTA MENSUAL', 'FACTURACION ANUAL', 'H/S', 'H/A', 'H/S BILLING', 'CODIGO TRABAJADOR', 'NUMERO TRABAJADORES', 'TAREAS EXTRAS', 'KM/S', 'COSTE EXTRA/SEMANA']]
df.to_csv(dir + "/data_gi_billing.csv")

df_billing_worker.to_csv(dir + "/data_billing_worker.csv")

# =============================================================================
#   EXPORT SERFEY
# =============================================================================

df = df_workers[df_workers['TIPO'] == 'MANTENIMIENTO']
df = df[['NOMBRE', 'CONVENIO', 'H/S', 'CATEGORIA', 'ANTIGUEDAD', 'PLUSES', 'LOCALIDAD', 'EMPRESA']]
df = df_workers[df_workers['TIPO'] == 'MOVILIDAD']
df = df[['NOMBRE', 'CONVENIO', 'H/S', 'CATEGORIA', 'ANTIGUEDAD', 'PLUSES', 'LOCALIDAD', 'EMPRESA', 'VEHICULO EMPRESA', 'TAREAS GENERALES']]
df = df_workers[df_workers['TIPO'] == 'ADMINISTRATIVO']
df = df[['NOMBRE', 'CONVENIO', 'H/S', 'CATEGORIA', 'ANTIGUEDAD', 'PLUSES', 'LOCALIDAD', 'EMPRESA']]

df = df_lsf_billing
df['EMPRESA'] = 'Limpiezas y Servicios La Fragatina, SL'
df = df[['NOMBRE', 'EMPRESA', 'TIPO CLIENTE', 'LOCALIDAD', 'CLIENTE PUBLICO', 'CUOTA MENSUAL', 'FACTURACION ANUAL', 'H/S', 'H/A', 'TAREAS EXTRAS', 'COSTE EXTRA/SEMANA']]
df = df_lsac_billing
df['EMPRESA'] = 'Limpiezas y Servicios Aragon y Cataluña, SL'
df = df[['NOMBRE', 'EMPRESA', 'TIPO CLIENTE', 'LOCALIDAD', 'CLIENTE PUBLICO', 'CUOTA MENSUAL', 'FACTURACION ANUAL', 'H/S', 'H/A', 'TAREAS EXTRAS', 'COSTE EXTRA/SEMANA']]
df = df_gi_billing
df['EMPRESA'] = 'Gestión Integral de Limpieza, SC'
df = df[['NOMBRE', 'EMPRESA', 'TIPO CLIENTE', 'LOCALIDAD', 'CLIENTE PUBLICO', 'CUOTA MENSUAL', 'FACTURACION ANUAL', 'H/S', 'H/A', 'TAREAS EXTRAS', 'COSTE EXTRA/SEMANA']]


