import pandas as pd

# Supongamos que este es el archivo CSV que has proporcionado
input_file = 'INVEN.870_ORACLE.csv'
output_file = 'INVENTARIO.csv'

# Carga el archivo CSV original
df = pd.read_csv(input_file, sep='|')

# Crear un nuevo DataFrame con la estructura requerida llenando los campos con los valores correspondientes
nuevo_df = pd.DataFrame({
    'headings': ['INV_INICIAL'] * len(df),
    'shipment_nbr': ['INV_INICIAL'] * len(df),
    'facility_code': df['Almacén'],  # Mapea el valor de "Almacén" al campo "facility_code"
    'company_code': df['Empresa'] * len(df),
    'trailer_nbr': [''] * len(df),
    'action_code': ['CREATE'] * len(df),
    'ref_nbr': [''] * len(df),
    'shipment_type': [''] * len(df),
    'load_nbr': [''] * len(df),
    'manifest_nbr': [''] * len(df),
    'trailer_type': [''] * len(df),
    'vendor_info': [''] * len(df),
    'origin_info': [''] * len(df),
    'origin_code': [''] * len(df),
    'orig_shipped_units': [''] * len(df),
    'lock_code': [''] * len(df),
    'shipped_date': df['Fecha inventario'],  # Mapea el valor de "Fecha inventario" al campo "shipped_date"
    'orig_shipped_lpns': [''] * len(df),
    'cust_field_1': [''] * len(df),
    'cust_field_2': [''] * len(df),
    'cust_field_3': [''] * len(df),
    'cust_field_4': [''] * len(df),
    'cust_field_5': [''] * len(df),
    'sold_to_legal_name': [''] * len(df),
    'returned_from_facility_code': [''] * len(df),
    'cust_date_1': [''] * len(df),
    'cust_date_2': [''] * len(df),
    'cust_date_3': [''] * len(df),
    'cust_date_4': [''] * len(df),
    'cust_date_5': [''] * len(df),
    'cust_decimal_1': [''] * len(df),
    'cust_decimal_2': [''] * len(df),
    'cust_decimal_3': [''] * len(df),
    'cust_decimal_4': [''] * len(df),
    'cust_decimal_5': [''] * len(df),
    'cust_number_1': [''] * len(df),
    'cust_number_2': [''] * len(df),
    'cust_number_3': [''] * len(df),
    'cust_number_4': [''] * len(df),
    'cust_number_5': [''] * len(df),
    'cust_long_text_1': [''] * len(df),
    'cust_long_text_2': [''] * len(df),
    'cust_long_text_3': [''] * len(df),
    'cust_short_text_1': [''] * len(df),
    'cust_short_text_2': [''] * len(df),
    'cust_short_text_3': [''] * len(df),
    'cust_short_text_4': [''] * len(df),
    'cust_short_text_5': [''] * len(df),
    'cust_short_text_6': [''] * len(df),
    'cust_short_text_7': [''] * len(df),
    'cust_short_text_8': [''] * len(df),
    'cust_short_text_9': [''] * len(df),
    'cust_short_text_10': [''] * len(df),
    'cust_short_text_11': [''] * len(df),
    'cust_short_text_12': [''] * len(df)
})

# Guardar el nuevo DataFrame en un archivo CSV
nuevo_df.to_csv(output_file, index=False, sep='\t')

print(f'Archivo con header modificado guardado en {output_file}')
