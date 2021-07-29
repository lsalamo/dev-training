import pandas as pd
import os
import shutil

# =============================================================================
# VARIABLES
# =============================================================================
os.chdir('/Users/luis.salamo/Documents/github enterprise/python-training/lsf')
DIR_PARENT = os.getcwd()
DIR_EXPORT = 'export' 


# =============================================================================
# WORKDERS
# =============================================================================

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
df_workers['CODIGO'] = df_workers['CODIGO'].astype(str)

# =============================================================================
# BILLING
# =============================================================================

# get data
def get_billing_data(file):
    df = pd.read_csv(DIR_PARENT + '/' + file, header=None)  
    df.rename(
        columns={
            0: 'NOMBRE',
            1: 'TIPO CLIENTE',
            2: 'LOCALIDAD',
            3: 'CLIENTE PUBLICO',
            4: 'CUOTA MENSUAL',
            5: 'FACTURACION ANUAL',
            6: 'CODIGO TRABAJADOR',
            7: 'H/S',
            8: 'KM',
            9: 'TAREAS EXTRAS'
        },
        inplace=True
    )
    df['CODIGO'] = 'CLIENT_' + (df.index + 1).astype(str)
    df['CODIGO TRABAJADOR'].fillna('', inplace=True)
    df['NUMERO TRABAJADORES'] = df['CODIGO TRABAJADOR'].apply(lambda x: 0 if not x else len(x.split(',')))
    df['H/S BILLING'] = df['H/S'] / df['NUMERO TRABAJADORES']    
    # df.set_index('CODIGO', inplace=True)
    return df

df_lsf_billing = get_billing_data('lsf_billing.csv')
df_lsac_billing = get_billing_data('lsac_billing.csv')

# QA WORKERS
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
    return new_df

df_lsf_billing_qa = get_billing_data_split_worker(df_lsf_billing,'CODIGO TRABAJADOR',',')
df_lsac_billing_qa = get_billing_data_split_worker(df_lsac_billing,'CODIGO TRABAJADOR',',')

def get_billing_worker(df1, df2):  
    df1 = df1.groupby(by='CODIGO TRABAJADOR')['H/S BILLING'].sum().to_frame()
    df1.rename(columns={'H/S BILLING': 'H/S LSF'},inplace=True)
    df2 = df2.groupby(by='CODIGO TRABAJADOR')['H/S BILLING'].sum().to_frame()
    df2.rename(columns={'H/S BILLING': 'H/S LSAC'},inplace=True)

    df = pd.merge(df1, df2, how='outer', on = 'CODIGO TRABAJADOR')
    df["H/S BILLING"] = df.sum(axis=1)
    return df
    
df_billing_worker_qa = get_billing_worker(df_lsf_billing_qa, df_lsac_billing_qa)
df_billing_worker = pd.merge(df_workers, df_billing_worker_qa, how='left', left_on = 'CODIGO', right_on='CODIGO TRABAJADOR')
df_billing_worker['H/S GAP'] = df_billing_worker['H/S BILLING'] - df_billing_worker['H/S']  
df_billing_worker = df_billing_worker[['CODIGO', 'NOMBRE', 'EMPRESA', 'H/S', 'H/S LSF', 'H/S LSAC', 'H/S BILLING', 'H/S GAP']]

# =============================================================================
#   RESULT
# =============================================================================

# df_billing.set_index('CODIGO', inplace=True)

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
    'df_workers_management': df_workers_management,
    'df_billing_worker': df_billing_worker
}

# =============================================================================
#   EXPORT CSV
# =============================================================================

dir = os.path.join(DIR_PARENT, DIR_EXPORT)
if os.path.isdir(dir):
    shutil.rmtree(dir)
os.makedirs(dir)
df_workers.to_csv(dir + "/data_workers.csv")
