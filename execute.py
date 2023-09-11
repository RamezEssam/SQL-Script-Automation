import pyodbc
import configparser
import logging
import sys
import time
import os




def execute(script_path,output_file_path):

    config= configparser.ConfigParser()
    
    logging.basicConfig(filename="D:\\work-automation\\automation-logs.txt",level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    current_wd = os.getcwd()
    

    try:
        config.read("D:\\work-automation\\DBConfig.config")
    except IOError as e:
        logging.error(f"{e}")


    driver= config.get("DB Configs","DRIVER")
    server=config.get("DB Configs","SERVER")
    port=config.get("DB Configs","PORT")
    db=config.get("DB Configs","DATABASE")
    uid=config.get("DB Configs","UID")
    pwd=config.get("DB Configs","PWD")


    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"PORT={port};"
        f"DATABASE={db};"
        f"UID={uid};"
        f"PWD={pwd};"
    )

    try:
        connection = pyodbc.connect(connection_string)
        logging.info(f"Connected to {db} Successfully!")
    except pyodbc.Error as e:
        logging.error(f"{e}")


    cursor = connection.cursor()
    try:
        logging.info(f"Opening {script_path} ...")
        with open(script_path, 'r') as sql_file:
            query=sql_file.read()
        
    except IOError as e:
        logging.error(f"{e}")

    try:
        logging.info("Executing query...")
        cursor.execute(query)
        output_file_path = f"{current_wd}\{output_file_path}-{time.strftime('%Y-%m-%d', time.localtime(time.time()))}.txt"
        logging.info(f"Writing {cursor.rowcount} rows to {output_file_path}...")
        try:
            with open(output_file_path, 'w',encoding='utf-8') as result_file:
                column_names = [column[0] for column in cursor.description]
                column_names = '\t'.join(column_names)
                result_file.write(f"{column_names}\n")
                while True:
                    try:
                        row = cursor.fetchone()
                    except pyodbc.Error as e:
                        logging.error(f"{e}")
                    if row is None:
                        break
                    row = [str(col) for col in row]
                    row_values = '\t'.join(row)
                    result_file.write(f"{row_values}\n")
        except IOError as e:
            logging.error(f"{e}")
        
        logging.info(f"Exported to {output_file_path}")
        cursor.close()
        connection.close()
    except pyodbc.Error as e:
        logging.error(f"{e}")
        cursor.close()
        connection.close()


if __name__ == "__main__":
    
    execute(sys.argv[1], sys.argv[2])

