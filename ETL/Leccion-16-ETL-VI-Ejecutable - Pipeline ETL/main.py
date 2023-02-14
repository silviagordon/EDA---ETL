import pandas as pd
import src.soporte as sp

df = pd.read_csv("etl2.csv", index_col = 0)
print("Fichero etl2.csv cargado correctamente.")
print(df.head(2))

df_ataques = pd.read_csv("attacks_limpieza_completa.csv", index_col = 0)
print("Fichero attacks_limpieza_completa.csv cargado correctamente.")
print(df.head(2))

# Creamos el diccionario con las coordenadas de los paises
diccionario_paises = {"usa":[39.7837304, -100.445882], "australia":[-24.7761086, 134.755], "south africa":[-28.8166236, 24.991639], "new zealand":[-41.5000831, 172.8344077], "papua new guinea":[-5.6816069, 144.2489081]}

# Creamos la instancia
etl = sp.Etl("meteo")
print("Creamos la instancia", etl)

# Hacemos la llamada a la API con el producto meteo
df_meteo = etl.llamar_api("meteo", diccionario_paises)
print("Comprobamos que está correcto", df_meteo.head(2))

# LLamamos a la función de limpieza de la columna rh_profile
df_rh_profile = etl.limpiar_rh_profile(df_meteo)
print("Comprobamos que está correcto", df_rh_profile.head(2))

# Llamamos a la función que limpian las columnas direction y speed de wind_profile
df_wind_direction_profile = etl.limpiar_wind_direction_profile(df_meteo)
print("Comprobamos que está correcto", df_wind_direction_profile.head(2))

df_wind_speed_profile = etl.limpiar_wind_speed_profile(df_meteo)

print("Comprobamos que está correcto", df_wind_speed_profile.head(2))

# Llamamos a la función que concatena las columnas profile en un solo dataframe
df_profile = etl.concatenar_columnas_profile(df_rh_profile, df_wind_direction_profile, df_wind_speed_profile)
print("Comprobamos que está correcto", df_profile.head(2))

# Llamamos a la función que elimina las columnas profile en el dataframe meteo
df_meteo = etl.eliminar_columnas_profile(df_meteo)
print("Comprobamos que está correcto", df_meteo.head(2))

# Llamamos a la función que une el dataframe meteo y profile en un nuevo dataframe
df_meteo_completo = etl.unir_df_meteo_profile(df_meteo, df_profile)
print("Comprobamos que está correcto", df_meteo_completo.head(2))

# Llamamos a la función que agrupa los países en el dataframe meteo_completo
df_grupo = etl.agrupar_paises_media(df_meteo_completo)
print("Comprobamos que está correcto", df_grupo.head(2))

# Llamamos a la función que une los dataframes meteo_completo y ataques
df_completo = etl.unir_df_grupo_ataques(df_grupo, df_ataques)
print("Comprobamos que está correcto", df_completo.head(2))

print("Proceso de extracción y transformación completado")

# Realizamos un reset_index de etl2.csv
df.reset_index(inplace=True)
print("Comprobamos que está correcto", df.head(2))

# Creamos las queries y variables para las tablas y contraseña
tabla_ataques = """ CREATE TABLE IF NOT EXISTS `tabla_ataques`(
                    `level_0` FLOAT NOT NULL,
                    `type` VARCHAR(50), 
                    `country` VARCHAR(50), 
                    `age` VARCHAR(50),
                    `species_` VARCHAR(50),
                    `fatal_Unknown` VARCHAR(50), 
                    `sex` VARCHAR(50), 
                    `species_.1` VARCHAR(50), 
                    `year` FLOAT, 
                    `age_NORM` FLOAT, 
                    PRIMARY KEY (`level_0`))
                    ENGINE = InnoDB; """

tabla_clima = """ CREATE TABLE IF NOT EXISTS `tabla_clima`(
                    `level_0` FLOAT NOT NULL,
                    `timepoint` INT, 
                    `cloudcover` INT, 
                    `highcloud` INT,
                    `midcloud` INT,
                    `lowcloud` INT,
                    `temp2m` INT, 
                    `lifted_index` INT, 
                    `rh2m` VARCHAR(50), 
                    `msl_pressure` INT,
                    `prec_amount` INT,
                    `snow_depth` INT,
                    `wind10m.direction` VARCHAR(50), 
                    `wind10m.speed` INT, 
                    `pais` VARCHAR(50),
                    PRIMARY KEY (`level_0`),
                    CONSTRAINT `fk_ataques_clima`
                    FOREIGN KEY (`level_0`)
                    REFERENCES `tabla_ataques` (`level_0`))
                    ENGINE = InnoDB; """


mybbdd = "tiburones2"

contraseña = "AlumnaAdalab"

# Creamos la instancia para la clase Sql

sql = sp.Sql(mybbdd,contraseña)
print("Creamos la instancia", sql)

# Llamamos a la función conexión al servidor

sql.conectar_bbdd(contraseña)

# Llamamos a la función crear base de datos

sql.crear_bbdd(mybbdd,contraseña)

# Llamamos a la función para crear las tablas

sql.crear_insertar_tabla(mybbdd, contraseña, tabla_ataques)
print("Tabla ataques creada correctamente.")

sql.crear_insertar_tabla(mybbdd, contraseña, tabla_clima)
print("Tabla clima creada correctamente.")

# Llamamos a la función de insertar datos
sql.insertar_registros_ataques(df, mybbdd, contraseña)
print("Registros insertados en tabla ataques creados correctamente.")

sql.insertar_registros_clima(df, mybbdd, contraseña)
print("Registros insertados en tabla clima creados correctamente.")

print("Proceso de carga en SQL completado")

