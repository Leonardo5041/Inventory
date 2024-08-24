import mysql.connector
import pandas as pd

connection = mysql.connector.connect(
    host = "10.115.4.42",
    user = "diegoa",
    password = "Oracl3Dieg0",
    database = "templatesDB"
)

cursor = connection.cursor()

query = (
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
SELECT ''                                       as 'ISS_InboundShipments_24A HEADER',
       'INV_INICIAL'                            as '[headings]',
       'INV_INICIAL'                            as shipment_nbr,
       ALMACEN                                  as facility_code,
       CODIGO_DE_EMPRESA                        as company_code,
       'CREATE'                                 as action_code,
       DATE_FORMAT(FECHA_DEL_INVENTARIO, '%Y%m%d') as shipped_date
FROM RankedInventario
WHERE rn = 1;
"""
)
cursor.execute(query)

# Fetch all the rows from the query
datos = cursor.fetchall()

# Obtener nombres de columnas
columnas = [desc[0] for desc in cursor.description]

df = pd.DataFrame(datos, columns=columnas)

df.to_csv('inventario_fisico.csv', index=False)

cursor.close()
connection.close()
