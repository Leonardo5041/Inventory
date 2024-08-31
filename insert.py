from datetime import datetime

import pandas as pd
import mysql.connector
from mysql.connector import Error

# Lee el archivo CSV
df = pd.read_csv('INVEN.840_ORACLE_CIG.csv', sep=',')

def handle_empty_value(value):
    if pd.isna(value) or value == '':
        return None  # Inserta NULL en MySQL
    return value

# Establece la conexión a la base de datos MySQL
def insert_into_db():
    try:
        connection = mysql.connector.connect(
            host="10.115.4.89",
            user="diegoa",
            password="Oracl3Dieg0",
            database="templatesDB"
        )

        if connection.is_connected():
            cursor = connection.cursor()
            # imprime en consola la hora de inicio de la inserción
            print("Iniciando inserción de datos...", datetime.now())

            # Recorre el DataFrame y realiza la inserción
            for index, row in df.iterrows():
                sql = """INSERT INTO ST_INVENTARIO_FISICO (CODIGO_DE_EMPRESA, FECHA_DEL_INVENTARIO, ALMACEN, BARCODE, PIEZAS, CODIGO_DE_UBICACION, CODIGO_LPN, FECHA_DE_CADUCIDAD, NO_LOTE, RDM)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (
                    handle_empty_value(row['Empresa']),
                    handle_empty_value(row['Fecha inventario']),
                    handle_empty_value(row['Almacén']),
                    handle_empty_value(row['Barcode']),
                    handle_empty_value(row['Piezas']),
                    handle_empty_value(row['ID Ubicación']),
                    handle_empty_value(row['LPN']),
                    handle_empty_value(row['Fecha caducidad']),
                    handle_empty_value(row['No. Lote']),
                    handle_empty_value(row['RDM'])
                )
                cursor.execute(sql, values)

            # Confirma los cambios en la base de datos
            connection.commit()
            print("Finalizando inserción de datos...", datetime.now())
            print("Datos insertados exitosamente")

    except Error as e:
        print("Error al conectarse a MySQL", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión a MySQL cerrada")