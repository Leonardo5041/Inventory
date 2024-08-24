import pandas as pd
import mysql.connector

from test import columnas

connection = mysql.connector.connect(
    host = "10.115.4.42",
    user = "diegoa",
    password = "Oracl3Dieg0",
    database = "templatesDB"
)

cursor = connection.cursor()

query_header = (
"""
WITH RankedInventario AS (SELECT CASE
                                     WHEN FECHA_DEL_INVENTARIO LIKE '__/__/____'
                                         THEN STR_TO_DATE(FECHA_DEL_INVENTARIO, '%d/%m/%Y')
                                     WHEN FECHA_DEL_INVENTARIO LIKE '________'
                                         THEN STR_TO_DATE(FECHA_DEL_INVENTARIO, '%Y%m%d')
                                     ELSE FECHA_DEL_INVENTARIO -- Dejar la fecha como está si no coincide con los patrones
                                     END                                                                   AS FECHA_DEL_INVENTARIO,
                                 ALMACEN,
                                 CODIGO_DE_EMPRESA,
                                 ROW_NUMBER() OVER (PARTITION BY CODIGO_LPN ORDER BY FECHA_DEL_INVENTARIO) AS rn
                          FROM ST_INVENTARIO_FISICO)
SELECT
    #[headings]	shipment_nbr	facility_code	company_code	trailer_nbr	action_code	ref_nbr	shipment_type	load_nbr	manifest_nbr	trailer_type	vendor_info	origin_info	origin_code	orig_shipped_units	lock_code	shipped_date	orig_shipped_lpns	cust_field_1	cust_field_2	cust_field_3	cust_field_4	cust_field_5	sold_to_legal_name	returned_from_facility_code	cust_date_1	cust_date_2	cust_date_3	cust_date_4	cust_date_5	cust_decimal_1	cust_decimal_2	cust_decimal_3	cust_decimal_4	cust_decimal_5	cust_number_1	cust_number_2	cust_number_3	cust_number_4	cust_number_5	cust_long_text_1	cust_long_text_2	cust_long_text_3	cust_short_text_1	cust_short_text_2	cust_short_text_3	cust_short_text_4	cust_short_text_5	cust_short_text_6	cust_short_text_7	cust_short_text_8	cust_short_text_9	cust_short_text_10	cust_short_text_11	cust_short_text_12
    'INV_INICIAL'                               as '[headings]',
    'INV_INICIAL'                               as shipment_nbr,
    ALMACEN                                     as facility_code,
    CODIGO_DE_EMPRESA                           as company_code,
    ''                                          as trailer_nbr,
    'CREATE'                                    as action_code,
    ''                                          as ref_nbr,
    ''                                          as shipment_type,
    ''                                          as load_nbr,
    ''                                          as manifest_nbr,
    ''                                          as trailer_type,
    ''                                          as vendor_info,
    ''                                          as origin_info,
    ''                                          as origin_code,
    ''                                          as orig_shipped_units,
    ''                                          as lock_code,
    DATE_FORMAT(FECHA_DEL_INVENTARIO, '%Y%m%d') as shipped_date,
    ''                                          as orig_shipped_lpns,
    ''                                          as cust_field_1,
    ''                                          as cust_field_2,
    ''                                          as cust_field_3,
    ''                                          as cust_field_4,
    ''                                          as cust_field_5,
    ''                                          as sold_to_legal_name,
    ''                                          as returned_from_facility_code,
    ''                                          as cust_date_1,
    ''                                          as cust_date_2,
    ''                                          as cust_date_3,
    ''                                          as cust_date_4,
    ''                                          as cust_date_5,
    ''                                          as cust_decimal_1,
    ''                                          as cust_decimal_2,
    ''                                          as cust_decimal_3,
    ''                                          as cust_decimal_4,
    ''                                          as cust_decimal_5,
    ''                                          as cust_number_1,
    ''                                          as cust_number_2,
    ''                                          as cust_number_3,
    ''                                          as cust_number_4,
    ''                                          as cust_number_5,
    ''                                          as cust_long_text_1,
    ''                                          as cust_long_text_2,
    ''                                          as cust_long_text_3,
    ''                                          as cust_short_text_1,
    ''                                          as cust_short_text_2,
    ''                                          as cust_short_text_3,
    ''                                          as cust_short_text_4,
    ''                                          as cust_short_text_5,
    ''                                          as cust_short_text_6,
    ''                                          as cust_short_text_7,
    ''                                          as cust_short_text_8,
    ''                                          as cust_short_text_9,
    ''                                          as cust_short_text_10,
    ''                                          as cust_short_text_11,
    ''                                          as cust_short_text_12
FROM RankedInventario
WHERE rn = 1;
"""
)

header_data = cursor.execute(query_header)

data_header = cursor.fetchall()

header_columns = [desc[0] for desc in cursor.description]


header_df = pd.DataFrame(data_header, columns=header_columns)

query_detail = (
    """
   SELECT 'INV_INICIAL'                                                   as '[headings]',
       ROW_NUMBER() OVER (PARTITION BY CODIGO_LPN ORDER BY CODIGO_LPN) AS seq_nbr,
       'CREATE'                                                        as action_code,
       CODIGO_LPN                                                      as lpn_nbr,
       ''                                                              as lpn_weight,
       ''                                                              as lpn_volume,
       ''                                                              as item_alternate_code,
       BARCODE                                                         as item_part_a,
       ''                                                              as item_part_b,
       ''                                                              as item_part_c,
       ''                                                              as item_part_d,
       ''                                                              as item_part_e,
       ''                                                              as item_part_f,
       ''                                                              as pre_pack_code,
       ''                                                              as pre_pack_ratio,
       ''                                                              as pre_pack_total_units,
       ''                                                              as invn_attr_a,
       ''                                                              as invn_attr_b,
       ''                                                              as invn_attr_c,
       PIEZAS                                                          as shipped_qty,
       ''                                                              as priority_date,
       ''                                                              as po_nbr,
       ''                                                              as pallet_nbr,
       ''                                                              as putaway_type,
       ''                                                              as expiry_date,
       ''                                                              as batch_nbr,
       ''                                                              as recv_xdock_facility_code,
       ''                                                              as cust_field_1,
       ''                                                              as cust_field_2,
       ''                                                              as cust_field_3,
       ''                                                              as cust_field_4,
       ''                                                              as cust_field_5,
       ''                                                              as lpn_is_physical_pallet_flg,
       ''                                                              as po_seq_nbr,
       ''                                                              as pre_pack_ratio_seq,
       ''                                                              as serial_nbr,
       ''                                                              as lpn_lock_code,
       ''                                                              as item_barcode,
       ''                                                              as uom,
       ''                                                              as lpn_length,
       ''                                                              as lpn_width,
       ''                                                              as lpn_height,
       ''                                                              as dtl_rcv_flg,
       ''                                                              as invn_attr_d,
       ''                                                              as invn_attr_e,
       ''                                                              as invn_attr_f,
       ''                                                              as invn_attr_g,
       ''                                                              as receipt_advice_line,
       ''                                                              as invn_attr_h,
       ''                                                              as invn_attr_i,
       ''                                                              as invn_attr_j,
       ''                                                              as invn_attr_k,
       ''                                                              as invn_attr_l,
       ''                                                              as invn_attr_m,
       ''                                                              as invn_attr_n,
       ''                                                              as invn_attr_o,
       ''                                                              as cust_date_1,
       ''                                                              as cust_date_2,
       ''                                                              as cust_date_3,
       ''                                                              as cust_date_4,
       ''                                                              as cust_date_5,
       ''                                                              as cust_decimal_1,
       ''                                                              as cust_decimal_2,
       ''                                                              as cust_decimal_3,
       ''                                                              as cust_decimal_4,
       ''                                                              as cust_decimal_5,
       ''                                                              as cust_number_1,
       ''                                                              as cust_number_2,
       ''                                                              as cust_number_3,
       ''                                                              as cust_number_4,
       ''                                                              as cust_number_5,
       ''                                                              as cust_long_text_1,
       ''                                                              as cust_long_text_2,
       ''                                                              as cust_long_text_3,
       ''                                                              as cust_short_text_1,
       ''                                                              as cust_short_text_2,
       ''                                                              as cust_short_text_3,
       ''                                                              as cust_short_text_4,
       ''                                                              as cust_short_text_5,
       ''                                                              as cust_short_text_6,
       ''                                                              as cust_short_text_7,
       ''                                                              as cust_short_text_8,
       ''                                                              as cust_short_text_9,
       ''                                                              as cust_short_text_10,
       ''                                                              as cust_short_text_11,
       ''                                                              as cust_short_text_12,
       ''                                                              as uom_code,
       ''                                                              as weight_uom_code,
       ''                                                              as volume_uom_code,
       ''                                                              as dimension_uom_code,
       ''                                                              as lpn_type,
       ''                                                              as line_schedule_nbrs,
       ''                                                              as marked_for_qc_flg
FROM ST_INVENTARIO_FISICO;
"""
)
# Crear el DataFrame para la sección DETAIL
detail_data = cursor.execute(query_detail)

detail_info = cursor.fetchall()

detail_columns = [desc[0] for desc in cursor.description]
detail_df = pd.DataFrame(detail_info, columns=detail_columns)

# Crear el DataFrame para la sección LLL_LocateLPNLock_24A

lll_query = (
    """
    SELECT
    CODIGO_DE_EMPRESA as 'company_code',
    ALMACEN as 'facility_code',
    CODIGO_LPN as 'lpn_nbr',
    '' as receipt_ts,
    BARCODE as 'location_barcode',
    '' as lock_code,
    '' as pallet_nbr,
    '' as pallet_position
    FROM ST_INVENTARIO_FISICO ORDER BY CODIGO_LPN;
    """
)

lll_data = cursor.execute(lll_query)

lll_info = cursor.fetchall()

lll_columns = [desc[0] for desc in cursor.description]

lll_locate_df = pd.DataFrame(lll_info, columns=lll_columns)

lll_locate_df.to_csv('LLL_LocateLPNLock_24A.csv', index=False)

# Guardar los DataFrames en un archivo CSV
with open('ISS_InboundShipments_24A.csv', 'w', newline='') as f:
    header_df.to_csv(f, index=False)
    detail_df.to_csv(f, index=False)

    #lll_locate_df.to_csv(f, index=False)

print("CSV generado correctamente.")
