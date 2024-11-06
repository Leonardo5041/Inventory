import mysql.connector
import pandas as pd


def generate_csv_active():
    # connection = mysql.connector.connect(
    #     host="10.115.4.89",
    #     user="diegoa",
    #     password="Oracl3Dieg0",
    #     database="templatesDB"
    # )
    connection = mysql.connector.connect(
            host="127.0.0.1",
            port="33060",
            user="root",
            password="admin",
            database="ORACLE"
        )
    cursor = connection.cursor()
    query_header = (
    """
    SELECT
        '[H1]'                               as '[headings]',
        CONCAT('INV_INICIAL_', DATE_FORMAT(NOW(), '%d%m%Y')) as shipment_nbr,
        ORACLE.CDT(CAST(ALMACEN AS UNSIGNED))                                     as facility_code,
        'GPOSAN'                          as company_code,
        ''                                          as trailer_nbr,
        'CREATE'                                    as action_code,
        ''                                          as ref_nbr,
        'INV'                                          as shipment_type,
        ''                                          as load_nbr,
        ''                                          as manifest_nbr,
        ''                                          as trailer_type,
        ''                                          as vendor_info,
        ''                                          as origin_info,
        ''                                          as origin_code,
        ''                                          as orig_shipped_units,
        ''                                          as lock_code,
        DATE_FORMAT(NOW(), '%Y%m%d') as shipped_date,
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
    FROM ST_INVENTARIO_FISICO LIMIT 1;
    """
    )

    header_data = cursor.execute(query_header)
    data_header = cursor.fetchall()
    header_columns = [desc[0] for desc in cursor.description]
    header_df = pd.DataFrame(data_header, columns=header_columns)
    query_detail = (
        """
        SELECT '[H2]' AS '[headings]',
           @n := @n + 1 AS seq_nbr,
           'CREATE' AS action_code,
           (
               SELECT MIN(CODIGO_LPN)
               FROM ST_INVENTARIO_FISICO I2
               WHERE I1.ALMACEN = I2.ALMACEN
                 AND I1.CODIGO_DE_UBICACION = I2.CODIGO_DE_UBICACION
           ) AS lpn_nbr,
           '' AS lpn_weight,
           '' AS lpn_volume,
           '' AS item_alternate_code,
           CONCAT(I1.CODIGO_DE_EMPRESA, I1.BARCODE) AS item_part_a,
           '' AS item_part_b,
           '' AS item_part_c,
           '' AS item_part_d,
           '' AS item_part_e,
           '' AS item_part_f,
           '' AS pre_pack_code,
           '' AS pre_pack_ratio,
           '' AS pre_pack_total_units,
           '' AS invn_attr_a,
           '' AS invn_attr_b,
           '' AS invn_attr_c,
           I1.PIEZAS AS shipped_qty,
           '' AS priority_date,
           '' AS po_nbr,
           '' AS pallet_nbr,
           '' AS putaway_type,
           '' AS expiry_date,
           '' AS batch_nbr,
           '' AS recv_xdock_facility_code,
           '' AS cust_field_1,
           '' AS cust_field_2,
           I1.RDM AS cust_field_3,
           '' AS cust_field_4,
           '' AS cust_field_5,
           '' AS lpn_is_physical_pallet_flg,
           '' AS po_seq_nbr,
           '' AS pre_pack_ratio_seq,
           '' AS serial_nbr,
           '' AS lpn_lock_code,
           '' AS item_barcode,
           '' AS uom,
           '' AS lpn_length,
           '' AS lpn_width,
           '' AS lpn_height,
           '' AS dtl_rcv_flg,
           '' AS invn_attr_d,
           '' AS invn_attr_e,
           '' AS invn_attr_f,
           '' AS invn_attr_g,
           '' AS receipt_advice_line,
           '' AS invn_attr_h,
           '' AS invn_attr_i,
           '' AS invn_attr_j,
           '' AS invn_attr_k,
           '' AS invn_attr_l,
           '' AS invn_attr_m,
           '' AS invn_attr_n,
           '' AS invn_attr_o,
           '' AS cust_date_1,
           '' AS cust_date_2,
           '' AS cust_date_3,
           '' AS cust_date_4,
           '' AS cust_date_5,
           '' AS cust_decimal_1,
           '' AS cust_decimal_2,
           '' AS cust_decimal_3,
           '' AS cust_decimal_4,
           '' AS cust_decimal_5,
           '' AS cust_number_1,
           '' AS cust_number_2,
           '' AS cust_number_3,
           '' AS cust_number_4,
           '' AS cust_number_5,
           '' AS cust_long_text_1,
           '' AS cust_long_text_2,
           '' AS cust_long_text_3,
           '' AS cust_short_text_1,
           '' AS cust_short_text_2,
           '' AS cust_short_text_3,
           '' AS cust_short_text_4,
           '' AS cust_short_text_5,
           '' AS cust_short_text_6,
           '' AS cust_short_text_7,
           '' AS cust_short_text_8,
           '' AS cust_short_text_9,
           '' AS cust_short_text_10,
           '' AS cust_short_text_11,
           '' AS cust_short_text_12,
           '' AS uom_code,
           '' AS weight_uom_code,
           '' AS volume_uom_code,
           '' AS dimension_uom_code,
           '' AS lpn_type,
           '' AS line_schedule_nbrs,
           '' AS marked_for_qc_flg
    FROM ST_INVENTARIO_FISICO I1
    CROSS JOIN (SELECT @n := 0) AS init
    INNER JOIN ST_UBICACIONES_ALM AS UBI
    ON
        I1.ALMACEN = UBI.ID_CDI
        AND
        I1.CODIGO_DE_UBICACION = UBI.barcode
    WHERE
        UBI.type = 'A'
    ORDER BY seq_nbr ASC;
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
        SELECT DISTINCT
        A.company_code,
        A.facility_code,
        MIN(A.lpn_nbr) AS lpn_nbr,
        '' as receipt_ts,
        A.location_barcode,
        '' as lock_code,
        '' as pallet_nbr,
        '' as pallet_position
    FROM (
        SELECT
            'GPOSAN' AS company_code,
            ORACLE.CDT(CAST(INV.ALMACEN AS UNSIGNED)) AS facility_code,
            INV.CODIGO_LPN AS lpn_nbr,
            INV.CODIGO_DE_UBICACION AS location_barcode
        FROM
            ST_INVENTARIO_FISICO AS INV
        INNER JOIN
            ST_UBICACIONES_ALM AS UBI
        ON
            INV.CODIGO_DE_UBICACION = UBI.barcode
            AND INV.ALMACEN = UBI.ID_CDI
        WHERE
            UBI.type = 'A' -- CAMBIAR POR 'R' PARA RESERVA
    ) AS A
    GROUP BY
        A.company_code,
        A.facility_code,
        A.location_barcode;
        """
    )

    lll_data = cursor.execute(lll_query)

    lll_info = cursor.fetchall()

    lll_columns = [desc[0] for desc in cursor.description]

    lll_locate_df = pd.DataFrame(lll_info, columns=lll_columns)

    lll_locate_df.to_csv('LLL_LocateLPNLock_24A_ACTIVO.csv', index=False)

    # Guardar los DataFrames en un archivo CSV
    with open('ISS_InboundShipments_24A_ACTIVO.csv', 'w', newline='') as f:
        header_df.to_csv(f, index=False)
        detail_df.to_csv(f, index=False)

        # lll_locate_df.to_csv(f, index=False)

    print("CSV generado correctamente.")
    cursor.close()
    connection.close()

def generate_csv_reserve():
    connection = mysql.connector.connect(
            host="127.0.0.1",
            port="33060",
            user="root",
            password="admin",
            database="ORACLE"
        )
    cursor = connection.cursor()
    query_header = (
    """
    SELECT
        '[H1]'                               as '[headings]',
        'INV_INICIAL_01092024'                               as shipment_nbr,
        ORACLE.CDT(CAST(ALMACEN AS UNSIGNED))                                     as facility_code,
        'GPOSAN'                          as company_code,
        ''                                          as trailer_nbr,
        'CREATE'                                    as action_code,
        ''                                          as ref_nbr,
        'INV'                                          as shipment_type,
        ''                                          as load_nbr,
        ''                                          as manifest_nbr,
        ''                                          as trailer_type,
        ''                                          as vendor_info,
        ''                                          as origin_info,
        ''                                          as origin_code,
        ''                                          as orig_shipped_units,
        ''                                          as lock_code,
        DATE_FORMAT(NOW(), '%Y%m%d') as shipped_date,
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
    FROM ST_INVENTARIO_FISICO LIMIT 1;
    """
    )

    header_data = cursor.execute(query_header)
    data_header = cursor.fetchall()
    header_columns = [desc[0] for desc in cursor.description]
    header_df = pd.DataFrame(data_header, columns=header_columns)
    query_detail = (
        """
        SELECT '[H2]' AS '[headings]',
           @n := @n + 1 AS seq_nbr,
           'CREATE' AS action_code,
           CODIGO_LPN AS lpn_nbr,
           '' AS lpn_weight,
           '' AS lpn_volume,
           '' AS item_alternate_code,
           CONCAT(I1.CODIGO_DE_EMPRESA, I1.BARCODE) AS item_part_a,
           '' AS item_part_b,
           '' AS item_part_c,
           '' AS item_part_d,
           '' AS item_part_e,
           '' AS item_part_f,
           '' AS pre_pack_code,
           '' AS pre_pack_ratio,
           '' AS pre_pack_total_units,
           '' AS invn_attr_a,
           '' AS invn_attr_b,
           '' AS invn_attr_c,
           I1.PIEZAS AS shipped_qty,
           '' AS priority_date,
           '' AS po_nbr,
           '' AS pallet_nbr,
           '' AS putaway_type,
           '' AS expiry_date,
           '' AS batch_nbr,
           '' AS recv_xdock_facility_code,
           '' AS cust_field_1,
           '' AS cust_field_2,
           I1.RDM AS cust_field_3,
           '' AS cust_field_4,
           '' AS cust_field_5,
           '' AS lpn_is_physical_pallet_flg,
           '' AS po_seq_nbr,
           '' AS pre_pack_ratio_seq,
           '' AS serial_nbr,
           '' AS lpn_lock_code,
           '' AS item_barcode,
           '' AS uom,
           '' AS lpn_length,
           '' AS lpn_width,
           '' AS lpn_height,
           '' AS dtl_rcv_flg,
           '' AS invn_attr_d,
           '' AS invn_attr_e,
           '' AS invn_attr_f,
           '' AS invn_attr_g,
           '' AS receipt_advice_line,
           '' AS invn_attr_h,
           '' AS invn_attr_i,
           '' AS invn_attr_j,
           '' AS invn_attr_k,
           '' AS invn_attr_l,
           '' AS invn_attr_m,
           '' AS invn_attr_n,
           '' AS invn_attr_o,
           '' AS cust_date_1,
           '' AS cust_date_2,
           '' AS cust_date_3,
           '' AS cust_date_4,
           '' AS cust_date_5,
           '' AS cust_decimal_1,
           '' AS cust_decimal_2,
           '' AS cust_decimal_3,
           '' AS cust_decimal_4,
           '' AS cust_decimal_5,
           '' AS cust_number_1,
           '' AS cust_number_2,
           '' AS cust_number_3,
           '' AS cust_number_4,
           '' AS cust_number_5,
           '' AS cust_long_text_1,
           '' AS cust_long_text_2,
           '' AS cust_long_text_3,
           '' AS cust_short_text_1,
           '' AS cust_short_text_2,
           '' AS cust_short_text_3,
           '' AS cust_short_text_4,
           '' AS cust_short_text_5,
           '' AS cust_short_text_6,
           '' AS cust_short_text_7,
           '' AS cust_short_text_8,
           '' AS cust_short_text_9,
           '' AS cust_short_text_10,
           '' AS cust_short_text_11,
           '' AS cust_short_text_12,
           '' AS uom_code,
           '' AS weight_uom_code,
           '' AS volume_uom_code,
           '' AS dimension_uom_code,
           '' AS lpn_type,
           '' AS line_schedule_nbrs,
           '' AS marked_for_qc_flg
    FROM ST_INVENTARIO_FISICO I1
    CROSS JOIN (SELECT @n := 0) AS init
    INNER JOIN ST_UBICACIONES_ALM AS UBI
    ON
        I1.CODIGO_DE_UBICACION = UBI.barcode
        AND I1.ALMACEN = UBI.ID_CDI
    WHERE
        UBI.type = 'R'
    ORDER BY seq_nbr ASC;
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
         SELECT DISTINCT 
    'GPOSAN' as 'company_code',
    ORACLE.CDT(CAST(ALMACEN AS UNSIGNED)) as 'facility_code',
    CODIGO_LPN as 'lpn_nbr',
    '' as receipt_ts,
    ST_INVENTARIO_FISICO.CODIGO_DE_UBICACION as 'location_barcode',
    '' as lock_code,
    '' as pallet_nbr,
    '' as pallet_position
    FROM ST_INVENTARIO_FISICO
    INNER JOIN ST_UBICACIONES_ALM UBI
    ON ST_INVENTARIO_FISICO.CODIGO_DE_UBICACION = UBI.barcode
    WHERE UBI.type = 'R'
    ORDER BY CODIGO_LPN;
    """
    )

    lll_data = cursor.execute(lll_query)

    lll_info = cursor.fetchall()

    lll_columns = [desc[0] for desc in cursor.description]

    lll_locate_df = pd.DataFrame(lll_info, columns=lll_columns)

    lll_locate_df.to_csv('LLL_LocateLPNLock_24A_RESERVA.csv', index=False)

    # Guardar los DataFrames en un archivo CSV
    with open('ISS_InboundShipments_24A_RESERVA.csv', 'w', newline='') as f:
        header_df.to_csv(f, index=False)
        detail_df.to_csv(f, index=False)

        # lll_locate_df.to_csv(f, index=False)

    print("CSV generado correctamente.")
    cursor.close()
    connection.close()


def generate_damage_csv():
    connection = mysql.connector.connect(
            host="127.0.0.1",
            port="33060",
            user="root",
            password="admin",
            database="ORACLE"
        )
    cursor = connection.cursor()
    query_header = (
        """
        SELECT
            '[H1]'                               as '[headings]',
            CONCAT('INV_INICIAL_', DATE_FORMAT(NOW(), '%d%m%Y')) as shipment_nbr,
            ORACLE.CDT(CAST(ALMACEN AS UNSIGNED))                                     as facility_code,
            'GPOSAN'                          as company_code,
            ''                                          as trailer_nbr,
            'CREATE'                                    as action_code,
            ''                                          as ref_nbr,
            'INV'                                          as shipment_type,
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
        FROM ST_INVENTARIO_FISICO LIMIT 1;
        """
    )

    header_data = cursor.execute(query_header)
    data_header = cursor.fetchall()
    header_columns = [desc[0] for desc in cursor.description]
    header_df = pd.DataFrame(data_header, columns=header_columns)
    query_detail = (
        """
        SELECT '[H2]' AS '[headings]',
           @n := @n + 1 AS seq_nbr,
           'CREATE' AS action_code,
           CODIGO_LPN AS lpn_nbr,
           '' AS lpn_weight,
           '' AS lpn_volume,
           '' AS item_alternate_code,
           CONCAT(I1.CODIGO_DE_EMPRESA, I1.BARCODE) AS item_part_a,
           '' AS item_part_b,
           '' AS item_part_c,
           '' AS item_part_d,
           '' AS item_part_e,
           '' AS item_part_f,
           '' AS pre_pack_code,
           '' AS pre_pack_ratio,
           '' AS pre_pack_total_units,
           '' AS invn_attr_a,
           '' AS invn_attr_b,
           '' AS invn_attr_c,
           I1.PIEZAS AS shipped_qty,
           '' AS priority_date,
           '' AS po_nbr,
           '' AS pallet_nbr,
           '' AS putaway_type,
           '' AS expiry_date,
           '' AS batch_nbr,
           '' AS recv_xdock_facility_code,
           '' AS cust_field_1,
           '' AS cust_field_2,
           I1.RDM AS cust_field_3,
           '' AS cust_field_4,
           '' AS cust_field_5,
           '' AS lpn_is_physical_pallet_flg,
           '' AS po_seq_nbr,
           '' AS pre_pack_ratio_seq,
           '' AS serial_nbr,
           '' AS lpn_lock_code,
           '' AS item_barcode,
           '' AS uom,
           '' AS lpn_length,
           '' AS lpn_width,
           '' AS lpn_height,
           '' AS dtl_rcv_flg,
           '' AS invn_attr_d,
           '' AS invn_attr_e,
           '' AS invn_attr_f,
           '' AS invn_attr_g,
           '' AS receipt_advice_line,
           '' AS invn_attr_h,
           '' AS invn_attr_i,
           '' AS invn_attr_j,
           '' AS invn_attr_k,
           '' AS invn_attr_l,
           '' AS invn_attr_m,
           '' AS invn_attr_n,
           '' AS invn_attr_o,
           '' AS cust_date_1,
           '' AS cust_date_2,
           '' AS cust_date_3,
           '' AS cust_date_4,
           '' AS cust_date_5,
           '' AS cust_decimal_1,
           '' AS cust_decimal_2,
           '' AS cust_decimal_3,
           '' AS cust_decimal_4,
           '' AS cust_decimal_5,
           '' AS cust_number_1,
           '' AS cust_number_2,
           '' AS cust_number_3,
           '' AS cust_number_4,
           '' AS cust_number_5,
           '' AS cust_long_text_1,
           '' AS cust_long_text_2,
           '' AS cust_long_text_3,
           '' AS cust_short_text_1,
           '' AS cust_short_text_2,
           '' AS cust_short_text_3,
           '' AS cust_short_text_4,
           '' AS cust_short_text_5,
           '' AS cust_short_text_6,
           '' AS cust_short_text_7,
           '' AS cust_short_text_8,
           '' AS cust_short_text_9,
           '' AS cust_short_text_10,
           '' AS cust_short_text_11,
           '' AS cust_short_text_12,
           '' AS uom_code,
           '' AS weight_uom_code,
           '' AS volume_uom_code,
           '' AS dimension_uom_code,
           '' AS lpn_type,
           '' AS line_schedule_nbrs,
           '' AS marked_for_qc_flg
    FROM ST_INVENTARIO_FISICO I1
    CROSS JOIN (SELECT @n := 0) AS init
    INNER JOIN ST_UBICACIONES_ALM AS UBI
    ON
        I1.CODIGO_DE_UBICACION = UBI.barcode
        AND I1.ALMACEN = UBI.ID_CDI
    WHERE
        I1.RDM != ''
        AND UBI.type = 'R'
    ORDER BY seq_nbr ASC;
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
    'GPOSAN' as 'company_code',
    ALMACEN as 'facility_code',
    CODIGO_LPN as 'lpn_nbr',
    '' as receipt_ts,
    ST_INVENTARIO_FISICO.CODIGO_DE_UBICACION as 'location_barcode',
    '' as lock_code,
    '' as pallet_nbr,
    '' as pallet_position
    FROM ST_INVENTARIO_FISICO
    INNER JOIN ST_UBICACIONES_ALM UBI
    ON ST_INVENTARIO_FISICO.CODIGO_DE_UBICACION = UBI.barcode
    WHERE ST_INVENTARIO_FISICO.RDM != ''
    AND UBI.type != 'E'
    ORDER BY CODIGO_LPN;
    """
    )

    lll_data = cursor.execute(lll_query)

    lll_info = cursor.fetchall()

    lll_columns = [desc[0] for desc in cursor.description]

    lll_locate_df = pd.DataFrame(lll_info, columns=lll_columns)

    lll_locate_df.to_csv('LLL_LocateLPNLock_24A_RDM.csv', index=False)

    # Guardar los DataFrames en un archivo CSV
    with open('ISS_InboundShipments_24A_RDM_Reserva.csv', 'w', newline='') as f:
        header_df.to_csv(f, index=False)
        detail_df.to_csv(f, index=False)

        # lll_locate_df.to_csv(f, index=False)

    print("CSV generado correctamente.")
    cursor.close()
    connection.close()


def generate_csv_return():
    # connection = mysql.connector.connect(
    #     host="10.115.4.89",
    #     user="diegoa",
    #     password="Oracl3Dieg0",
    #     database="templatesDB"
    # )
    connection = mysql.connector.connect(
            host="127.0.0.1",
            port="33060",
            user="root",
            password="admin",
            database="ORACLE"
        )
    cursor = connection.cursor()
    query_header = (
    """
    SELECT
        '[H1]'                               as '[headings]',
        CONCAT('INV_INICIAL_', DATE_FORMAT(NOW(), '%d%m%Y'))                               as shipment_nbr,
        ORACLE.CDT(CAST(ALMACEN AS UNSIGNED))                                     as facility_code,
        'GPOSAN'                          as company_code,
        ''                                          as trailer_nbr,
        'CREATE'                                    as action_code,
        ''                                          as ref_nbr,
        'INV'                                          as shipment_type,
        ''                                          as load_nbr,
        ''                                          as manifest_nbr,
        ''                                          as trailer_type,
        ''                                          as vendor_info,
        ''                                          as origin_info,
        ''                                          as origin_code,
        ''                                          as orig_shipped_units,
        ''                                          as lock_code,
        DATE_FORMAT(NOW(), '%Y%m%d') as shipped_date,
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
    FROM ST_INVENTARIO_FISICO LIMIT 1;
    """
    )

    header_data = cursor.execute(query_header)
    data_header = cursor.fetchall()
    header_columns = [desc[0] for desc in cursor.description]
    header_df = pd.DataFrame(data_header, columns=header_columns)
    query_detail = (
        """
                SELECT '[H2]' AS '[headings]',
           @n := @n + 1 AS seq_nbr,
           'CREATE' AS action_code,
           I1.CODIGO_LPN AS lpn_nbr,
           '' AS lpn_weight,
           '' AS lpn_volume,
           '' AS item_alternate_code,
           CONCAT(I1.CODIGO_DE_EMPRESA, I1.BARCODE) AS item_part_a,
           '' AS item_part_b,
           '' AS item_part_c,
           '' AS item_part_d,
           '' AS item_part_e,
           '' AS item_part_f,
           '' AS pre_pack_code,
           '' AS pre_pack_ratio,
           '' AS pre_pack_total_units,
           '' AS invn_attr_a,
           '' AS invn_attr_b,
           '' AS invn_attr_c,
           I1.PIEZAS AS shipped_qty,
           '' AS priority_date,
           '' AS po_nbr,
           '' AS pallet_nbr,
           '' AS putaway_type,
           '' AS expiry_date,
           '' AS batch_nbr,
           '' AS recv_xdock_facility_code,
           '' AS cust_field_1,
           '' AS cust_field_2,
           '' AS cust_field_3,
           '' AS cust_field_4,
           I1.No_LOTE AS cust_field_5,
           '' AS lpn_is_physical_pallet_flg,
           '' AS po_seq_nbr,
           '' AS pre_pack_ratio_seq,
           '' AS serial_nbr,
           '' AS lpn_lock_code,
           '' AS item_barcode,
           '' AS uom,
           '' AS lpn_length,
           '' AS lpn_width,
           '' AS lpn_height,
           '' AS dtl_rcv_flg,
           '' AS invn_attr_d,
           '' AS invn_attr_e,
           '' AS invn_attr_f,
           '' AS invn_attr_g,
           '' AS receipt_advice_line,
           '' AS invn_attr_h,
           '' AS invn_attr_i,
           '' AS invn_attr_j,
           '' AS invn_attr_k,
           '' AS invn_attr_l,
           '' AS invn_attr_m,
           '' AS invn_attr_n,
           '' AS invn_attr_o,
           '' AS cust_date_1,
           '' AS cust_date_2,
           '' AS cust_date_3,
           '' AS cust_date_4,
           '' AS cust_date_5,
           '' AS cust_decimal_1,
           '' AS cust_decimal_2,
           '' AS cust_decimal_3,
           '' AS cust_decimal_4,
           '' AS cust_decimal_5,
           '' AS cust_number_1,
           '' AS cust_number_2,
           '' AS cust_number_3,
           '' AS cust_number_4,
           '' AS cust_number_5,
           '' AS cust_long_text_1,
           '' AS cust_long_text_2,
           '' AS cust_long_text_3,
           '' AS cust_short_text_1,
           '' AS cust_short_text_2,
           '' AS cust_short_text_3,
           '' AS cust_short_text_4,
           '' AS cust_short_text_5,
           '' AS cust_short_text_6,
           '' AS cust_short_text_7,
           '' AS cust_short_text_8,
           '' AS cust_short_text_9,
           '' AS cust_short_text_10,
           '' AS cust_short_text_11,
           '' AS cust_short_text_12,
           '' AS uom_code,
           '' AS weight_uom_code,
           '' AS volume_uom_code,
           '' AS dimension_uom_code,
           '' AS lpn_type,
           '' AS line_schedule_nbrs,
           '' AS marked_for_qc_flg
    FROM ST_INVENTARIO_FISICO I1
    CROSS JOIN (SELECT @n := 0) AS init
    INNER JOIN ST_UBICACIONES_ALM AS UBI
    ON
        I1.ALMACEN = UBI.ID_CDI
        AND
        I1.CODIGO_DE_UBICACION = UBI.barcode
    WHERE
        UBI.type = 'E'
    ORDER BY seq_nbr ASC;
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
        SELECT DISTINCT 
    'GPOSAN' as 'company_code',
    ORACLE.CDT(CAST(ALMACEN AS UNSIGNED)) as 'facility_code',
    CODIGO_LPN as 'lpn_nbr',
    '' as receipt_ts,
    ST_INVENTARIO_FISICO.CODIGO_DE_UBICACION as 'location_barcode',
    '' as lock_code,
    '' as pallet_nbr,
    '' as pallet_position
    FROM ST_INVENTARIO_FISICO
    INNER JOIN ST_UBICACIONES_ALM UBI
    ON ST_INVENTARIO_FISICO.CODIGO_DE_UBICACION = UBI.barcode
    WHERE UBI.type = 'E'
    ORDER BY CODIGO_LPN
        """
    )

    lll_data = cursor.execute(lll_query)

    lll_info = cursor.fetchall()

    lll_columns = [desc[0] for desc in cursor.description]

    lll_locate_df = pd.DataFrame(lll_info, columns=lll_columns)

    lll_locate_df.to_csv('LLL_LocateLPNLock_24A_DEVOL.csv', index=False)

    # Guardar los DataFrames en un archivo CSV
    with open('ISS_InboundShipments_24A_DEVOL.csv', 'w', newline='') as f:
        header_df.to_csv(f, index=False)
        detail_df.to_csv(f, index=False)

        # lll_locate_df.to_csv(f, index=False)

    print("CSV generado correctamente.")
    cursor.close()
    connection.close()