import pandas as pd
pd.options.display.max_columns=None
import requests
import numpy as np
import mysql.connector
from mysql.connector import errorcode

class Etl:
    # constructor
    def __init__(self, producto):
        
        self.producto = producto

    # Método para llamar a la API
    def llamar_api(self, producto, diccionario):
        df_meteo= pd.DataFrame()

        for k,v in diccionario.items():
            df_meteo1 = pd.DataFrame()
            # Los values de latitud y longitud serían = latitud: v[0] y longitud: v[1]. 
            producto = "meteo"
            response = requests.get(url = f'http://www.7timer.info/bin/api.pl?lon=-{v[1]}&lat={v[0]}&product={self.producto}&output=json') 
            df_meteo1 = pd.DataFrame.from_dict(pd.json_normalize(response.json()["dataseries"]))
            df_meteo1["pais"] = k

            df_meteo = pd.concat([df_meteo1, df_meteo], axis=0, ignore_index=True) 
            
            if response.status_code == 200:
                print('La peticion se ha realizado correctamente, se ha devuelto el código de estado:',response.status_code,' y como razón del código de estado: ',response.reason)
            elif response.status_code == 402:
                print('No se ha podido autorizar usario, se ha devuelto el código de estado:', response.status_code,' y como razón del código de estado: ',response.reason)
            elif response.status_code == 404:
                print('Algo ha salido mal, el recurso no se ha encontrado,se ha devuelto el código de estado:', response.status_code,' y como razón del código de estado: ',response.reason)
            else:
                print('Algo inesperado ha ocurrido, se ha devuelto el código de estado:', response.status_code,' y como razón del código de estado: ',response.reason) 
        
        print(f"df_meteo tiene un formato de (filas,columnas): {df_meteo.shape}")
        return df_meteo
        

    def limpiar_rh_profile(self,df_meteo):
        # Función que limpia la columna rh_profile del dataframe
       
        # Resetear el índice del dataframe para no tener columnas repetidas
        df_meteo.reset_index(inplace=True)
        # Convertimos a serie la columna que en su interior tiene un diccionario
        df_rh = df_meteo["rh_profile"].apply(pd.Series)

        # Creamos un dataframe vacío para insertar cada uno de los values del diccionario
        df_rh_profile = pd.DataFrame()
        for i in range(len(df_rh.columns)): 
            #aplicamos el apply,extraemos el valore de la key "layer" y lo almacenamos en una variable que convertimos a string 
            nombre = "rh_" + str(df_rh[i].apply(pd.Series)["layer"][0]) 
            #hacemos lo mismo con una variable que se llame valores para "guardar" los valores de la celda
            valores = list(df_rh[i].apply(pd.Series)["rh"] )
            #usamos el método insert de los dataframes para ir añadiendo esta información a el dataframe con la información del clima. 
            df_rh_profile.insert(i, nombre, valores)
        
        print(f"df_rh_profile tiene un formato de (filas,columnas): {df_rh_profile.shape}")
        return df_rh_profile

    def limpiar_wind_direction_profile(self, df_meteo):
        # Función que limpia la columna wind_profile del dataframe

        # Resetear el índice del dataframe para no tener columnas repetidas
        df_meteo.reset_index(inplace=True)        
        # Convertimos a serie la columna que en su interior tiene un diccionario
        df_wind = df_meteo["wind_profile"].apply(pd.Series)

        # Creamos un dataframe vacío para insertar cada uno de los values del diccionario
        df_wind_direction_profile = pd.DataFrame()
        for i in range(len(df_wind.columns)): 
            #aplicamos el apply,extraemos el valore de la key "layer" y lo almacenamos en una variable que convertimos a string 
            nombre = "wind_direction_" + str(df_wind[i].apply(pd.Series)["layer"][0]) 
            #hacemos lo mismo con una variable que se llame valores para "guardar" los valores de la celda
            valores = list(df_wind[i].apply(pd.Series)["direction"])
            #usamos el método insert de los dataframes para ir añadiendo esta información a el dataframe con la información del clima. 
            df_wind_direction_profile.insert(i, nombre, valores)
        print(f"df_wind_direction_profile tiene un formato de (filas,columnas): {df_wind_direction_profile.shape}")
        return df_wind_direction_profile
    
    def limpiar_wind_speed_profile(self, df_meteo):
        
        # Convertimos a serie la columna que en su interior tiene un diccionario
        df_wind = df_meteo["wind_profile"].apply(pd.Series)
        
        # Creamos un dataframe vacío para insertar cada uno de los values del diccionario
        df_wind_speed_profile = pd.DataFrame()
        for i in range(len(df_wind.columns)): 
            #aplicamos el apply,extraemos el valore de la key "layer" y lo almacenamos en una variable que convertimos a string 
            nombre = "wind_speed_" + str(df_wind[i].apply(pd.Series)["layer"][0]) 
            #hacemos lo mismo con una variable que se llame valores para "guardar" los valores de la celda
            valores = list(df_wind[i].apply(pd.Series)["speed"])
            #usamos el método insert de los dataframes para ir añadiendo esta información a el dataframe con la información del clima. 
            df_wind_speed_profile.insert(i, nombre, valores)
        
        print(f"df_wind_speed_profile tiene un formato de (filas,columnas): {df_wind_speed_profile.shape}")
        return df_wind_speed_profile

    def concatenar_columnas_profile(self, df_rh_profile, df_wind_direction_profile, df_wind_speed_profile):
        df_profile = pd.concat([df_wind_direction_profile, df_wind_speed_profile, df_rh_profile], axis=1)
        print(f"df_profile tiene una formato de (filas,columnas): {df_profile.shape}")
        return df_profile

    def eliminar_columnas_profile(self, df_meteo):
        df_meteo = df_meteo.drop(["rh_profile", "wind_profile"], axis=1)
        print(f"Ahora df_meteo tiene una formato de (filas,columnas): {df_meteo.shape}")
        return df_meteo

    def unir_df_meteo_profile(self, df_meteo, df_profile):
        df_meteo_completo = df_meteo.join(df_profile, how="inner", on="index")
        print(f"Ahora df_meteo_completo tiene una formato de (filas,columnas): {df_meteo_completo.shape}")
        return df_meteo_completo

    def agrupar_paises_media(self, df_meteo_completo):
        df_grupo = df_meteo_completo.groupby("pais")[df_meteo_completo.columns].mean().reset_index()
        print(f"df_grupo tiene una formato de (filas,columnas): {df_grupo.shape}")
        return df_grupo

    def unir_df_grupo_ataques(self, df_grupo, df_ataques):
        df_completo = df_ataques.merge(df_grupo, how="inner", right_on="pais", left_on="country")
        print(f"Ahora df_completo tiene una formato de (filas,columnas): {df_completo.shape}")
        return df_completo

class Sql:

    def __init__(self, nombre_bbdd, password):
        self.nombre_bbdd = nombre_bbdd
        self.password = password

    # Importamos librería
    def conectar_bbdd(self,password):
        # Función para comprobar si se establece la conexión con el servidor
        # Parámetro password: string con la contraseña del servidor
        try:
            cnx = mysql.connector.connect(user = 'root', password = self.password, host = '127.0.0.1')
        # en el caso de que haya errores
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            cnx.close()
            # Si devuelve close todo ha funcionado correctamente
            return "Tienes conexión con el servidor"

    def crear_bbdd(self, nombre_bbdd, password):
        # Función para crear una base de datos
        # Parámetro nombre_bbdd: string con el nombre de la base de datos a crear
        # password: string con la contraseña del servidor
        mydb = mysql.connector.connect(user = 'root', password = f"{self.password}", host = '127.0.0.1')
        #print("Conexión realizada con éxito")
        mycursor = mydb.cursor()
        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            print(mycursor)
            print("La base de datos se ha creado correctamente")
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
        else:
            mycursor.close()
            mydb.close()

    def crear_insertar_tabla(self, nombre_bbdd, password, query):
        # Función para crear una tabla dentro de una base de datos
        # Parámetro nombre_bbdd: string con el nombre de la base de datos a insertar la tabla
        # password: string con la contraseña del servidor
        # query: consulta en SQL de creación de tabla
        cnx = mysql.connector.connect(user = 'root', password = self.password, host = '127.0.0.1', database = self.nombre_bbdd)
        mycursor = cnx.cursor()
        try:
            mycursor.execute(query)
            #print(mycursor)
            cnx.commit()
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
        else:
            mycursor.close()
            cnx.close()
    
    def insertar_registros_ataques(self, df, nombre_bbdd, password):
        for indice, fila in df.iterrows():
            query_ataques = f""" INSERT INTO `tabla_ataques`(`level_0`, `type`, `country`, `age`, `species_`, `fatal_Unknown`, `sex`, `species_.1`, `year`, `age_NORM`)
                                VALUES ('{fila["level_0"]}', '{fila["type"]}', '{fila["country"]}', '{fila["age"]}', '{fila["species_"]}', '{fila["fatal_Unknown"]}', '{fila["sex"]}', '{fila["species_.1"]}', '{fila["year"]}', '{fila["age_NORM"]}');    
                            """
            self.crear_insertar_tabla(nombre_bbdd, password, query_ataques)


    def insertar_registros_clima(self,df, nombre_bbdd, password):
        for indice, fila in df.iterrows():
            query_clima = f""" INSERT INTO `tabla_clima`(`level_0`, `timepoint`, `cloudcover`, `highcloud`, `midcloud`, `lowcloud`, `temp2m`, `lifted_index`, `rh2m`, `msl_pressure`, `prec_amount`, `snow_depth`, `wind10m.direction`, `wind10m.speed`, `pais`)
                                VALUES ('{fila["level_0"]}', '{fila["timepoint"]}', '{fila["cloudcover"]}', '{fila["highcloud"]}', '{fila["midcloud"]}', '{fila["lowcloud"]}', '{fila["temp2m"]}', '{fila["lifted_index"]}', '{fila["rh2m"]}', '{fila["msl_pressure"]}', '{fila["prec_amount"]}', '{fila["snow_depth"]}', '{fila["wind10m.direction"]}', '{fila["wind10m.speed"]}', '{fila["pais"]}');          
                            """
            self.crear_insertar_tabla(nombre_bbdd, password, query_clima)