import pandas as pd
import mysql.connector
from functions.csv_generate import generate_csv_active
from functions.csv_generate import generate_csv_reserve
from functions.csv_generate import generate_damage_csv
from insert import insert_into_db

def main():
    choice = 0
    while choice != 4:
        print("1. Insertar datos en la base de datos")
        print("2. Generar CSV de Inventario")
        print("3. Salir")
        choice = int(input("Ingrese una opción: "))
        if choice == 1:
            insert_into_db()
        if choice == 2:
            generate_csv()
        elif choice == 3:
            print("Saliendo...")
            break
        else:
            print("Opción inválida. Intente de nuevo.")


def generate_csv():
    type = input("Ingrese el tipo de inventario (1. Activo, 2. Reserva, 3. RDM): ")
    if type == '1':
        generate_csv_active()
    elif type == '2':
        generate_csv_reserve()
    elif type == '3':
        generate_damage_csv()
    else:
        print("Tipo de inventario inválido. Intente de nuevo.")
if __name__ == '__main__':
    main()