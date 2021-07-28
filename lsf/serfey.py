import pandas as pd
import os

# Directory
os.chdir('/Users/luis.salamo/Documents/github enterprise/python-training/lsf')
DIR_PARENT = os.getcwd()
DIR_EXPORT = 'export' 

# read data worker
df_lsf_workers = pd.read_csv(DIR_PARENT + "/lsf_workers.csv", header=None)  
df_lsf_workers["EMPRESA"] = 'Limpiezas y Servicios La Fragatina, SL'
df_lsac_workers = pd.read_csv(DIR_PARENT + "/lsac_workers.csv", header=None)  
df_lsac_workers["EMPRESA"] = 'Limpiezas y Servicios Aragon y Cataluña, SL'

df_workers = pd.concat([df_lsf_workers, df_lsac_workers], ignore_index=True)
df_workers = df_workers.drop([2, 3], axis=1)
df_workers["CONVENIO"] = 'Aragón'
df_workers["PLUSES"] = 'PREGUNTAR A ELSA'
df_workers["LOCALIDAD"] = 'PREGUNTAR A ELSA'
df_workers["VEHICULO EMPRESA"] = 'PREGUNTAR A ELSA'
df_workers["CATEGORIA"] = 'PREGUNTAR A ELSA CATEGORIA PROFESIONAL'
df_workers["TAREAS GENERALES"] = 'PREGUNTAR FAMILIA SALAMO ESTEVE'
df_workers.rename(
    columns={
        0: 'CODIGO',
        1: 'NOMBRE',
        4: 'ANTIGUEDAD',
        5: 'TELEFONO',
        6: 'H/S',
        7: 'TIPO CONTRATO',
        8: 'BAJA LABORAL',
        9: 'TOTAL DEVENGOS',
        10: 'COSTE S.S.'
    },
    inplace=True
)

# read data billing
df_billing = pd.read_csv(DIR_PARENT + "/billing.csv", header=None)  
df_billing.rename(
    columns={
        0: 'CODIGO',
        1: 'NOMBRE',
        2: 'H/S',
        3: 'CODIGO TRABAJADOR ALL'
    },
    inplace=True
)
df_billing['WORKERS'] = df_billing['CODIGO TRABAJADOR ALL'].str.split(',').apply(lambda x: len(x))
df_billing['H/S BILLING'] = df_billing['H/S'] / df_billing['WORKERS']

# total hours by worker into billing
df_billing_workers = pd.DataFrame(df_billing['CODIGO TRABAJADOR ALL'].str.split(',').tolist(), index=df_billing['CODIGO']).stack()
df_billing_workers = df_billing_workers.reset_index([0, 'CODIGO'])
df_billing_workers.columns = ['CODIGO', 'CODIGO TRABAJADOR']
df_billing_workers = pd.merge(df_billing, df_billing_workers, on='CODIGO')
df_billing_groupby_workers = df_billing_workers.groupby(by='CODIGO TRABAJADOR')['H/S BILLING'].sum()
df_billing_groupby_workers = df_billing_groupby_workers.to_frame().reset_index()
df_billing_groupby_workers['CODIGO TRABAJADOR'] = df_billing_groupby_workers['CODIGO TRABAJADOR'].astype('int64', copy=False)

df_billing_qa = pd.merge(df_workers, df_billing_groupby_workers, how='outer', left_on = 'CODIGO', right_on='CODIGO TRABAJADOR')
df_billing_qa = df_billing_qa[['CODIGO', 'NOMBRE', 'EMPRESA', 'H/S', 'H/S BILLING']]
df_billing_qa['GAP'] = df_billing_qa['H/S BILLING'] - df_billing_qa['H/S']

# =============================================================================
#   RESULT
# =============================================================================

df_billing.set_index('CODIGO', inplace=True)

df_workers_maintenance = df_workers[~df_workers['NOMBRE'].isin(['ELSA KLEIN CASTAÑ','KAREN LUCIA FERNANDEZ SAAVEDRA'])]
df_workers_maintenance = df_workers_maintenance[['CODIGO', 'CONVENIO', 'H/S', 'ANTIGUEDAD', 'PLUSES', 'LOCALIDAD', 'EMPRESA']]
df_workers_maintenance.set_index('CODIGO', inplace=True)

df_workers_mobile = df_workers[~df_workers['NOMBRE'].isin(['ELSA KLEIN CASTAÑ','KAREN LUCIA FERNANDEZ SAAVEDRA'])]
df_workers_mobile = df_workers_mobile[['CODIGO', 'CONVENIO', 'H/S', 'ANTIGUEDAD', 'CATEGORIA', 'PLUSES', 'VEHICULO EMPRESA', 'TAREAS GENERALES']]
df_workers_mobile.set_index('CODIGO', inplace=True)

df_workers_management = df_workers[df_workers['NOMBRE'].isin(['ELSA KLEIN CASTAÑ','KAREN LUCIA FERNANDEZ SAAVEDRA'])]
df_workers_management = df_workers_management[['CODIGO', 'CONVENIO', 'H/S', 'ANTIGUEDAD', 'PLUSES', 'LOCALIDAD', 'EMPRESA']]
df_workers_management.set_index('CODIGO', inplace=True)

result = { 
    'total_workers': df_workers.shape[0], 
    'df_workers': df_workers,
    'df_workers_maintenance': df_workers_maintenance,
    'df_workers_mobile': df_workers_mobile,
    'df_workers_management': df_workers_management
}
