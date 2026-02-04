 






DESARROLLO DE LA INICIATIVA
CONNECTA VLCi
Expediente:  094/21-SP



Caso de uso 12. Intercambio de datos entre el ayuntamiento de Valencia-AENA
Plan de actuación







05/12/2024
 

Control de versiones del documento
Versión	Descripción	Autor	Fecha
1.0	Versión Inicial	Telefónica	05/12/2024
			


 
Índice
Índice de ilustraciones	4
1	Introducción	5
1.1	Objetivo	5
1.2	Destinatarios y Confidencialidad	5
1.3	Mantenimiento y Revisiones	5
2	Modelo de datos	6
2.1	Modelo NGSI	6
2.1.1	Entidades principales	6
2.2	Modelo DB	14
2.2.1	Scripts inicialización DB	14
3	Extracción de información de fuentes externas	19
3.1	Objetivo de la ETL de la API AENA para vertical de Centros de Transporte	19
3.2	Elementos necesarios	19
3.3	Método de autenticación	20
3.4	Contenido del directorio	21
3.5	Variables de entorno	21
3.6	Funcionamiento	22
3.7	Ejecución del proceso de ETL	23
3.7.1	Ejecución de la ETL principal	23
3.7.2	Modelo resultante	23
3.7.3	Error durante la ejecución	23
3.8	Código de la ETL	23
4	Cuadro de mandos	34
4.1	Panel general del caso de uso	34
4.2	Panel “mapa”	35
4.3	Panel de análisis	36
4.4	Panel de comparativa	36

 
Índice de ilustraciones
Ilustración 1. Autenticación	20
Ilustración 2. Panel general del caso de uso (visión general)	34
Ilustración 3. Panel mapa de aeropuertos	35
Ilustración 4. Panel de análisis	36
Ilustración 5. Panel de comparativa	36
 
1	Introducción
1.1	Objetivo
El presente documento describe los elementos desarrollados y desplegados en plataforma para la construcción del caso de uso 12: Intercambio de datos entre el ayuntamiento de Valencia y AENA.
1.2	Destinatarios y Confidencialidad
El destinatario del presente documento es la Dirección Técnica del proyecto designada a tal efecto por Red.es y el Ayto. de València.
El acceso a este documento está restringido a la Dirección Técnica del proyecto. Cualquier acceso fuera de este ámbito, debe ser autorizado de forma expresa por la Dirección Técnica de Red.es y el Ayto. de València. 
1.3	Mantenimiento y Revisiones
Este documento es elaborado y mantenido por Telefónica.
2	Modelo de datos 
Se detalla a continuación el modelo de datos que definirá la información integrada en la plataforma, proveniente a través de procesos de extracción de la información de los datos de transporte aéreo de diferentes fuentes.
2.1	Modelo NGSI
El modelo de información NGSI representa información de contexto como entidades, que tienen propiedades y se relacionan con otras entidades. Cada entidad y relación recibe una referencia única como identificador, lo que hace que los datos correspondientes se puedan exportar como conjuntos de datos de datos enlazados. Para este caso de uso, se parte de modelos estándar FIWARE, adaptados a la necesidad del proyecto.
2.1.1	Entidades principales
Hubs
La entidad Hubs proporciona un modelo de datos genérico para la gestión de cualquier tipo de centro de transporte como aeropuertos, puertos o estaciones de bus. El identificador viene dado por 4 letras que identifiquen el hub.
Referencia de campos Hubs:
Atributo	ngsiType	dbType	description	example	extra	unit	range
TimeeInstant	DateTime	timestamp with time zone	Fecha de actualización de la entidad.	"2020-01-01T00:00:00.00Z"	-	-	-
iataCode	Text	text	Código IATA del hub.	"BCN"	-	-	-
hubType	Text	text	Tipo del hub.	"airport"	-	-	[airport, port, trainstation, busstation]
city	Text	text	Ciudad donde se encuentra el hub	"EL PRAT DE LLOBREGAT"	-	-	-
location	geo:json	geometry	Posición geográfica en formato geo:json [lon, lat].	{"type": "Point", "coordinates": [-3.641971, 40.547555]}	-	-	-
zip	Text	text	Código postal para mancomunado	"NA"	-	-	-
zone	Text	text	Zona para mancomunado	"NA"	-	-	-
district	Text	text	Distrito para mancomunado	"NA"	-	-	-
municipality	Text	text	En servicios mancomunados, indica el municipio al que pertenecen los datos. Se recomienda usar el nombre de dominio utilizado por el ayuntamiento	"NA"	-	-	-
province	Text	text	Provincia para mancomunado	"NA"	-	-	-
region	Text	text	Region/Comarca para mancomunado	"NA"	-	-	-
community	Text	text	Comunidad autónoma para mancomunado	"NA"	-	-	-
country	Text	text	País para mancomunado	"NA"	-	-	-
streetAddress	Text	text	Calle donde está ubicada la entidad	"Carretera CL 615, km. 55"	-	-	-
postalCode	Text	text	Código postal donde está ubicada la entidad	"NA"	-	-	-
addressLocality	Text	text	Localidad donde está ubicada la entidad	"NA"	-	-	-
addressRegion	Text	text	Región/provincia donde está ubicada la entidad	"NA"	-	-	-
addressCommunity	Text	text	Comunidad autónoma donde está ubicada la entidad	"NA"	-	-	-
addressCountry	Text	text	País donde está ubicada la entidad	"NA"	-	-	-
name	Text	text	Nombre del sensor	"NA"	-	-	-
description	TextUnrestricted	text	Descripción del evento	"NA"	-	-	-

Ejemplo de Hubs (en NGSIv2):
{
    "id": "GCFV",
    "type": "Hubs",
    "TimeInstant": {
        "type": "DateTime",
        "value": "2020-01-01T00:00:00.00Z"
    },
    "iataCode": {
        "type": "Text",
        "value": "BCN"
    },
    "hubType": {
        "type": "Text",
        "value": "airport"
    },
    "city": {
        "type": "Text",
        "value": "EL PRAT DE LLOBREGAT"
    },
    "location": {
        "type": "geo:json",
        "value": {
            "type": "Point",
            "coordinates": [
                -3.641971,
                40.547555
            ]
        }
    },
    "zip": {
        "type": "Text",
        "value": "NA"
    },
    "zone": {
        "type": "Text",
        "value": "NA"
    },
    "district": {
        "type": "Text",
        "value": "NA"
    },
    "municipality": {
        "type": "Text",
        "value": "NA"
    },
    "province": {
        "type": "Text",
        "value": "NA"
    },
    "region": {
        "type": "Text",
        "value": "NA"
    },
    "community": {
        "type": "Text",
        "value": "NA"
    },
    "country": {
        "type": "Text",
        "value": "NA"
    },
    "streetAddress": {
        "type": "Text",
        "value": "Carretera CL 615, km. 55"
    },
    "postalCode": {
        "type": "Text",
        "value": "NA"
    },
    "addressLocality": {
        "type": "Text",
        "value": "NA"
    },
    "addressRegion": {
        "type": "Text",
        "value": "NA"
    },
    "addressCommunity": {
        "type": "Text",
        "value": "NA"
    },
    "addressCountry": {
        "type": "Text",
        "value": "NA"
    },
    "name": {
        "type": "Text",
        "value": "NA"
    },
    "description": {
        "type": "TextUnrestricted",
        "value": "NA"
    }
}

Transports
La entidad Transport proporciona un modelo de datos genérico para la gestión de cualquier tipo de transporte. El indentificador viene dado por 'TR' (de TRansport) o TR_API seguido del id del Transport, dependiendo si la medida viene por la ETL AENA o AENA_API respectivamente.
Referencia de campos Transports:
Atributo	ngsiType	dbType	description	example	extra	unit	range
calculationPeriodFrom	DateTime	timestamp with time zone	Fecha de inicio de la agregación de tiempo en la que ocurre el transporte.	"2020-01-01T00:00:00.00Z"	-	-	-
calculationPeriodTo	DateTime	timestamp with time zone	Fecha de fin de la agregación de tiempo en la que ocurre el transporte.	"2020-12-31T23:59:59.00Z"	-	-	-
transportMean	Text	text	Medio de transporte.	"airplane"	-	-	[airplane, ship, train, bus]
transportType	Text	text	Tipo de transporte. Por ejemplo pasajeros por escala o pasajeros por compaía.	"passengers-company"	-	-	[passengers-scale, passengers-company]
baseHub	Text	text	ID del hub base utilizado como referencia entre el Transport y la entidad Hub	"HAXXXX"	-	-	-
actor	TextUnrestricted	text	Actor que interviene en el transporte. Dependiendo del transporttype puede hacer referencia a distintos actores como compañia o hub de escala.	"VUELING AIRLINES, S.A."	-	-	-
movementType	Text	text	Tipo de movimiento del transporte.	"LLEGADA"	-	-	[LLEGADA,SALIDA]
transportCode	Text	text	Número del transporte asignado para identificar el transporte específico. Puede ser número de vuelo, bus.. etc.	"IB2142"	-	-	
baseCountry	Text	text	País del hub base.	"ESPA\u00d1A"	-	-	-
actorCountry	TextUnrestricted	text	País de procedencia donde opera el actor.	"ALEMANIA"	-	-	-
iataOrigin	Text	text	Código IATA de la terminal de origen del transporte. Puede ser un aeropuerto, estación de tren, terminal de autobús, etc. Aplica solo para LLEGADA.	"ZRH"	-	-	
year	Text	text	Año en el que se realiza el transporte.	"2020"	-	-	-
month	Text	text	Mes en el que se realiza el transporte.	"Enero"	-	-	-
kpiValue	Number	double precision	Valor del indicador en número de unidades. Número de pasajeros del transporte.	200	-	Pasajeros	0-n
name	Text	text	Nombre del sensor	"NA"	-	-	-
description	TextUnrestricted	text	Descripción del evento	"NA"	-	-	-

Ejemplo de Transports (en NGSIv2):
{
    "id": "TR_HALEMG",
    "type": "Transports",
    "calculationPeriodFrom": {
        "type": "DateTime",
        "value": "2020-01-01T00:00:00.00Z"
    },
    "calculationPeriodTo": {
        "type": "DateTime",
        "value": "2020-12-31T23:59:59.00Z"
    },
    "transportMean": {
        "type": "Text",
        "value": "airplane"
    },
    "transportType": {
        "type": "Text",
        "value": "passengers-company"
    },
    "baseHub": {
        "type": "Text",
        "value": "HAXXXX"
    },
    "actor": {
        "type": "TextUnrestricted",
        "value": "VUELING AIRLINES, S.A."
    },
    "movementType": {
        "type": "Text",
        "value": "LLEGADA"
    },
    "transportCode": {
        "type": "Text",
        "value": "IB2142"
    },
    "baseCountry": {
        "type": "Text",
        "value": "ESPAÑA"
    },
    "actorCountry": {
        "type": "TextUnrestricted",
        "value": "ALEMANIA"
    },
    "iataOrigin": {
        "type": "Text",
        "value": "ZRH"
    },
    "year": {
        "type": "Text",
        "value": "2020"
    },
    "month": {
        "type": "Text",
        "value": "Enero"
    },
    "kpiValue": {
        "type": "Number",
        "value": 200
    },
    "name": {
        "type": "Text",
        "value": "NA"
    },
    "description": {
        "type": "TextUnrestricted",
        "value": "NA"
    }
}
2.2	Modelo DB
Para la persistencia de los datos del caso de uso se han desplegado las siguientes tablas en el datasource PostgreSQL:
Ficheros SQL asociados al modelo Hubs:
•	hubs_lastdata.sql: Fichero SQL del flujo lastdata (tipo FLOW_LASTDATA) en modelo Hubs
Ficheros SQL asociados al modelo Transports
•	transports_historic.sql: Fichero SQL del flujo historic (tipo FLOW_HISTORIC) en modelo Transports
Además, se han incorporado las siguientes vistas:
•	transports_historic_join.sql: Fichero SQL del flujo historic_join (tipo FLOW_JOIN_VIEW)

2.2.1	Scripts inicialización DB

CREATE TABLE IF NOT EXISTS :target_schema.transporthubs_hubs_lastdata (
  timeinstant timestamp with time zone,
  iatacode text,
  hubtype text,
  city text,
  location geometry,
  zip text,
  zone text,
  district text,
  municipality text,
  province text,
  region text,
  community text,
  country text,
  streetaddress text,
  postalcode text,
  addresslocality text,
  addressregion text,
  addresscommunity text,
  addresscountry text,
  name text,
  description text,
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT transporthubs_hubs_lastdata_pkey PRIMARY KEY (entityid)
);

CREATE TABLE IF NOT EXISTS :target_schema.transporthubs_transports (
  calculationperiodfrom timestamp with time zone,
  calculationperiodto timestamp with time zone,
  transportmean text,
  transporttype text,
  basehub text,
  actor text,
  movementtype text,
  transportcode text,
  basecountry text,
  actorcountry text,
  iataorigin text,
  year text,
  month text,
  kpivalue double precision,
  name text,
  description text,
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT transporthubs_transports_pkey PRIMARY KEY (calculationperiodfrom, entityid, transporttype, actor, movementtype, actorcountry)
);

-- TimeScale: create a hypertable
\set target_table_str $$:target_schema.transporthubs_transports$$
SELECT create_hypertable(:target_table_str,'calculationperiodfrom', chunk_time_interval => INTERVAL '3 months');

CREATE MATERIALIZED VIEW :target_schema.transporthubs_transports_day_lt 
WITH (timescaledb.continuous)
AS
SELECT
  time_bucket('1 day', calculationperiodfrom) AS date,
  DATE_PART('day', time_bucket('1 day', MAX(calculationperiodfrom) AT TIME ZONE 'UTC')) AS day,
  DATE_PART('week', time_bucket('1 day', MAX(calculationperiodfrom) AT TIME ZONE 'UTC')) AS week,
  DATE_PART('month', time_bucket('1 day', MAX(calculationperiodfrom) AT TIME ZONE 'UTC')) AS month,
  DATE_PART('quarter', time_bucket('1 day', MAX(calculationperiodfrom) AT TIME ZONE 'UTC')) AS quarter,
  DATE_PART('year', time_bucket('1 day', MAX(calculationperiodfrom) AT TIME ZONE 'UTC')) AS year,
  COUNT(calculationperiodfrom) AS countperperiod,
----
  MIN(kpivalue) AS minkpivalue,
  AVG(kpivalue) AS avgkpivalue,
  PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY kpivalue) AS medkpivalue,
  MAX(kpivalue) AS maxkpivalue,
  SUM(kpivalue) AS sumkpivalue,
  STDDEV(kpivalue) AS devkpivalue,
  VARIANCE(kpivalue) AS varkpivalue,
----
  entityid
FROM :target_schema.transporthubs_transports
GROUP BY date,
  entityid
ORDER BY
  entityid
 WITH NO DATA;

-- Remove if exists aggregate policy
\set target_mv_str $$:target_schema.transporthubs_transports_day_lt$$
SELECT remove_continuous_aggregate_policy(
:target_mv_str,
if_exists => true
);

CREATE OR REPLACE VIEW :target_schema.transporthubs_transports_historic_join AS
  SELECT
    left_table.entityid,
    left_table.entitytype,
    left_table.name,
    left_table.description,
    left_table.fiwareservicepath,
    left_table.calculationperiodfrom,
    left_table.calculationperiodto,
    left_table.transportmean,
    left_table.transporttype,
    left_table.basehub,
    left_table.actor,
    left_table.movementtype,
    left_table.transportcode,
    left_table.basecountry,
    left_table.actorcountry,
    left_table.year,
    left_table.month,
    left_table.kpivalue,
    left_table.iataorigin,
    right_table.name as hubname,
    right_table.description as hubdescription,
    right_table.iatacode,
    right_table.hubtype,
    right_table.city,
    right_table.location,
    right_table.zip,
    right_table.zone,
    right_table.district,
    right_table.municipality,
    right_table.province,
    right_table.region,
    right_table.community,
    right_table.country,
    right_table.streetaddress,
    right_table.postalcode,
    right_table.addresslocality,
    right_table.addressregion,
    right_table.addresscommunity,
    right_table.addresscountry
  FROM
    :target_schema.transporthubs_transports left_table
  INNER JOIN
    :target_schema.transporthubs_hubs_lastdata right_table
  ON left_table.baseHub = right_table.entityid
;
3	Extracción de información de fuentes externas
La API provee información de los vuelos de aeropuertos a todas las ciudades mediante la API de AENA, específicamente en las siguientes URL’s:
URL BASE: https://globalservicesqa.aena.es/smartcities-api/v1/
Servicios:
•	/vuelos-llegada/{aeropuerto}: Lista de vuelos programados de llegada.
•	/vuelos-salida/{aeropuerto}: Lista de vuelos programados de salida.
3.1	Objetivo de la ETL de la API AENA para vertical de Centros de Transporte
El objetivo de la ETL es extraer los datos que se comparten por parte de los aeropuertos a todas las ciudades mediante la API de AENA.
3.2	Elementos necesarios
Software
Para poder ejecutar el proceso de ETL en un equipo es necesario disponer del siguiente software:
Software	Versión
Python	>= 3.8
pip	*
Dentro del directorio con el código del proceso (etls) se encuentra un fichero requirements.txt que contiene las dependencias que necesita el script para funcionar adecuadamente. Es recomendable crear un entorno virtual de Python (virtualenv), acceder al mismo e instalar dichas dependencias mediante el comando pip install -r requirements.txt (recuerde ubicarse en su shell dentro del directorio etls).
Ejemplo:
  $ cd transporthubs-vertical/etls/AENA_API
  $ virtualenv venv
  $ source venv/bin/activate
  (venv)$ pip install -r requirements.txt

Endpoints API
Las llamadas a la API están protegidas mediante el uso de JSON Web Tokens (abreviado JWT), un estándar abierto basado en JSON para la creación de tokens de acceso que permiten la propagación de identidad y privilegios.
3.3	Método de autenticación
Autenticación
Se realizará una primera llamada de tipo POST para obtener el token de acceso. Será necesario autenticarse en el API Manager mediante el uso de una autorización básica en la que se incluirán un token que identificará al usuario que realiza la invocación. Los datos necesarios para generar el token serán proporcionados por AENA.
•	URL para la obtención del token: https://globalservicesqa.aena.es/token
•	Parametros: Se debe incluir el parametro “grant_type” con el valor “client_credentials”
URL final: https://globalservicesqa.aena.es/token?grant_type=client_credentials
En los headers de la petición se debe de incluir una cabecera con el token de autenticación de tipo “Basic”.
•	Valor de la cabecera será una cadena codificada en Base64 con lo siguiente: consumer_key:consumer_secret
La consumer_key y la consumer_secret las debe proporcionar AENA.
 
Ilustración 1. Autenticación


Obtención de la información
Una vez obtenido el token de acceso en el API MANAGER, se podrán realizar el resto de peticiones, añadiendo dicho token en la autorización de la llamada. La autorización será de tipo Bearer Token
URL BASE: https://globalservicesqa.aena.es/smartcities-api/v1/
•	/smartcities-api/vuelos-llegada/{aeropuerto}: devuelve una lista con los vuelos programados de llegada al aeropuerto.
•	/smartcities-api/vuelos-salida/{aeropuerto}: devuelve una lista con los vuelos programados de salida al aeropuerto.
3.4	Contenido del directorio
La estructura de este directorio es la siguiente:
•	config.py: fichero de configuración de variables de entorno.
•	env.sample.json: fichero de ejemplo para la carga de variables de entorno (en despliegues SaaS).
•	env.secrets.sample.json: fichero de ejemplo para la carga de variables de entorno sensibles (en despliegues SaaS).
•	etl.py: script de ejecución para todo el proceso de la ETL.
•	requeriments.txt: fichero con las librerías necesarias para la ejecución de la ETL.
3.5	Variables de entorno
Para la configuración de la ETL, se han de crear unas variables de entorno antes de ejecutar el script de python. Esas variables se detallan en los siguientes apartados.
Variables de entorno Generales
Estas variables, afectan al proceso de la ETL a nivel de configuración de entorno:
•	ETL_LOG_LEVEL: Nivel de log en tiempo de ejecución de la ETL, por defecto es INFO
•	ETL_AENA_API_ENVIRONMENT_ENDPOINT_CB: El host y puerto del Context Broker (host:port), por defecto es http://endpoint_cb
•	ETL_AENA_API_ENVIRONMENT_KEYSTONE: El host y puerto del IDM (host:port), por defecto es http://keystone
•	ETL_AENA_API_ENVIRONMENT_SERVICE: Nombre del servicio (Fiware-Service), por defecto es service
•	ETL_AENA_API_ENVIRONMENT_SUBSERVICE: Nombre del subservicio , por defecto es /transporthubs
•	ETL_AENA_API_ENVIRONMENT_USER: Usuario (usado para la operación "Get auth token"), por defecto es user
•	ETL_AENA_API_ENVIRONMENT_PASSWORD: Contraseña (usado para la operación "Get auth token"), por defecto es password
Variables de entorno de Ajustes
Estas variables, hacen referencia a ajustes:
•	ETL_AENA_API_SETTINGS_POST_TIMEOUT: Define un timeout en los envíos al CB, por defecto es 10
•	ETL_AENA_API_SETTINGS_POST_RETRY_CONNECT: Define los reintentos de conexión en caso de timeout en los envíos al CB, por defecto es 3
•	ETL_AENA_API_SETTINGS_POST_RETRY_BACKOFF_FACTOR: Factor de espera de reenvío (progresivo), entre reintentos en los envíos al CB, por defecto es 20. Es un valor numérico entre el 0 y 120. Se usa para calcular el tiempo de espera entre cada reintento. El tiempo se calcula en función del factor y el número reintentos que lleva realizados: {backoff factor} * (2 ** ({number of total retries})). La documentación de URLLIB3
•	ETL_AENA_API_SETTINGS_POST_BATCH_PAGE_SIZE: Tamaño de las ráfagas en el envío de datos al CB, por defecto es 200.
•	ETL_AENA_API_SETTINGS_POST_SECONDS_SLEEP: Espera en segundos, entre cada envío de datos al CB, por defecto es 1
•	ETL_AENA_API_SETTINGS_AIRPORTS: Lista de aeropuertos separada por comas de los cuales se extraerá la información de la API.
•	ETL_AENA_API_SETTINGS_URL_API: Url base de la API en donde se extraerá la información de los diferentes endpoints, la url varia según el entorno, debe ser proporcionada por AENA, por defecto es https://globalservicesqa.aena.es/smartcities-api/v1/smartcities-api/
•	ETL_AENA_API_SETTINGS_URL_TOKEN: Url para generar el token de acceso, la url varia según el entorno, debe ser proporcionada por AENA, por defecto es https://globalservicesqa.aena.es/token
•	ETL_AENA_API_SETTINGS_CONSUMER_KEY: Dato necesaria para la construcción del token de acceso para extraer los datos de la API. Ver apartado Autenticación
•	ETL_AENA_API_SETTINGS_SECRET_KEY: Dato necesaria para la construcción del token de acceso para extraer los datos de la API. Ver apartado Autenticación

3.6	Funcionamiento
La ejecución de la ETL se realiza manualmente. Luego de obtener el token, se usa en las peticiones a los diferentes servicios mencionados anteriormente en donde se especifica el listado de aeropuertos que se quiere obtener la información mediante variables de entorno y posteriormente, la información obtenida de la API es adaptada al modelo de Transport.
3.7	Ejecución del proceso de ETL
3.7.1	Ejecución de la ETL principal
Pasos para la ejecución de la ETL:
1.	Definir las variables de entorno adecuadas en el archivo config.py tal como se indica en el apartado Variables de entorno.
2.	Asegurarse que se especifiquen correctamente las variables ETL_AENA_API_SETTINGS_CONSUMER_KEY y ETL_AENA_API_SETTINGS_SECRET_KEY ya que determinan la correcta generación del token.
3.	Iniciar el proceso situando su shell dentro del directorio etls/AENA_API. Tras esto, se ha de ejecutar el siguiente comando:
python etl.py
3.7.2	Modelo resultante
El proceso de ETL se encarga de leer de la API especificada y enviar al Context Broker la siguiente información:
•	Cada uno de los resultados de vuelos de llegada y vuelos de salida que representan de manera única el número de pasajeros que han realizado un trayecto determinado en un tiempo determinado. Esto se lleva a cabo a través de una entidad identificada por el aeropuerto base del trayecto (código OACI del aeropuerto) ej: TR_API_GCFV. Donde GCFV representa el HUB_ID para cada aeropuerto. La entidad es de tipo Transport. Desde el Context Broker cada registro persistirá a base de datos.
3.7.3	Error durante la ejecución
La etl se encarga de hacer la carga al Context Broker cuando la extracción de los datos para todos los aeropuertos especificados en la variable de configuración ETL_AENA_API_SETTINGS_AIRPORTS ha sido completada con éxito. Se identificó que cuando un aeropuerto es especificado de forma errónea puede dar error al hacer una petición al endpoint con un aeropuerto inexistente y afectar la carga o si un aeropuerto no existe en los datos de los hubs para la extracción del HUB_ID puede fallar. Por este motivo la carga se hace al final para garantizar que solo se envíen los datos cuando la información esté al 100%.
En caso de interrupción de la ejecución de la ETL o algún error inesperado se puede volver a realizar la misma carga de datos sin consecuencias adversas como duplicidad de registros en base de datos.
3.8	Código de la ETL
# -*- coding: utf-8 -*-

# Copyright 2022 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
# PROJECT: transporthubs-vertical
#
# This software and / or computer program has been developed by Telefónica Soluciones
# de Informática y Comunicaciones de España, S.A.U (hereinafter TSOL) and is protected
# as copyright by the applicable legislation on intellectual property.
#
# It belongs to TSOL, and / or its licensors, the exclusive rights of reproduction,
# distribution, public communication and transformation, and any economic right on it,
# all without prejudice of the moral rights of the authors mentioned above. It is expressly
# forbidden to decompile, disassemble, reverse engineer, sublicense or otherwise transmit
# by any means, translate or create derivative works of the software and / or computer
# programs, and perform with respect to all or part of such programs, any type of exploitation.
#
# Any use of all or part of the software and / or computer program will require the
# express written consent of TSOL. In all cases, it will be necessary to make
# an express reference to TSOL ownership in the software and / or computer
# program.
#
# Non-fulfillment of the provisions set forth herein and, in general, any violation of
# the peaceful possession and ownership of these rights will be prosecuted by the means
# provided in both Spanish and international law. TSOL reserves any civil or
# criminal actions it may exercise to protect its rights.


import calendar
import sys
import traceback
import requests.adapters
import config
import logging
import requests
import tc_etl_lib as tc
from datetime import datetime

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any

# get enviroment and settigns
etl_log_level = config.etl_log_level
endpoint_cb = config.endpoint_cb
keystone = config.keystone
service = config.service
subservice = config.subservice
user = config.user
password = config.password
timeout = config.post_timeout
retry_connect = config.post_retry_connect
retry_backoff_factor = config.post_retry_backoff_factor
batch_page_size = config.post_batch_page_size
seconds_sleep = config.post_seconds_sleep
airports = config.airports
url_base = config.url_base
url_token = config.url_token
consumer_key = config.consumer_key
secret_key = config.secret_key


# sets the logging configuration
logging.basicConfig(
    level=logging.getLevelName(etl_log_level),
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-AENA-API | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",  # noqa
    handlers=[
        logging.StreamHandler()
    ]
)

# get logger to use it
logger = logging.getLogger()

if not consumer_key or not secret_key:
    logger.error('Some critical configuration is missing')
    sys.exit(-1)


# HTTP Adapter
class TimeoutHTTPAdapter(requests.adapters.HTTPAdapter):
    """HTTP Adapter for global timeouts. See https://github.com/psf/requests/issues/2011"""

    def __init__(self, timeout=None, **kwargs):
        """Add support for timeout variable"""
        super().__init__(**kwargs)
        self.timeout = timeout

    def send(self, *args, **kwargs):
        """Inject timeout into request"""
        kwargs['timeout'] = self.timeout
        return super().send(*args, **kwargs)


@dataclass
class APIRequest():
    consumer_key: str = consumer_key
    secret_key: str = secret_key
    url_token: str = url_token
    url_base: str = url_base
    timeout: int = timeout
    max_retries: int = retry_connect
    token: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        self.session = requests.Session()
        retry_strategy = requests.adapters.Retry(
            total=self.max_retries,
            backoff_factor=self.timeout,
            status_forcelist=[400, 500, 502, 503, 504]
        )
        adapter = TimeoutHTTPAdapter(
            timeout=self.timeout, max_retries=retry_strategy
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.token = self._get_token()

    def _get_token(self):
        """Obtiene el token de acceso desde la API."""
        try:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            query_params = {"grant_type": "client_credentials"}
            response = requests.post(
                url=self.url_token, headers=headers,
                data=query_params, timeout=self.timeout,
                auth=(self.consumer_key, self.secret_key)
            )
            response.raise_for_status()
            token = response.json().get('access_token')
            if token:
                return token
            else:
                logger.error("Token not found in response.")
                raise RuntimeError("Token not found in response.")

        except (requests.Timeout, requests.ConnectionError):
            logger.error("Request timed out while getting token.")
            raise

        except requests.RequestException as e:
            logger.error(f"Error fetching data from {self.url_token}: {e}")
            raise

    def _fetch_data(self, endpoint) -> Optional[List[Dict[str, Any]]]:
        """Obtiene los datos desde un endpoint específico."""

        if not self.token:
            logger.error("No token available, unable to fetch data.")
            raise ValueError("No token available, unable to fetch data.")

        try:
            url = f"{self.url_base}{endpoint}"
            headers = {'Authorization': f'Bearer {self.token}'}
            response = self.session.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 401:
                logger.info("Token expired, refreshing token.")
                self.token = self._get_token()
                if not self.token:
                    logger.error("Failed to refresh token.")
                    raise ValueError("Failed to refresh token.")

                headers['Authorization'] = f'Bearer {self.token}'
                response = self.session.get(url, headers=headers, timeout=self.timeout)

            if not response.content:
                logger.warning(f"Empty response content from {url}")
                raise ValueError("Empty response content")

            response.raise_for_status()
            data = response.json()
            return data

        except requests.RequestException as e:
            logger.error(f"Error fetching data from {url}: {e}")
            raise

    def get_vuelos_llegada(self, aeropuerto: str, format: Callable) -> Optional[List[Dict[str, Any]]]:
        """Obtiene los vuelos de llegada para un aeropuerto específico."""
        endpoint = f"vuelos-llegada/{aeropuerto}"
        return format(self._fetch_data(endpoint))

    def get_vuelos_salida(self, aeropuerto: str, format: Callable) -> Optional[List[Dict[str, Any]]]:
        """Obtiene los vuelos de salida para un aeropuerto específico."""
        endpoint = f"vuelos-salida/{aeropuerto}"
        return format(self._fetch_data(endpoint))


@dataclass
class ContextBrokerService():
    auth_manager: tc.authManager
    cb_manager: tc.cbManager
    subservice: str

    def send_entities(self, entities,):
        with tc.orionStore(auth=self.auth_manager, cb=self.cb_manager, subservice=self.subservice) as store:
            store(entities)

    def get_entities(self, type: str):
        logger.info("*** RETRIEVING HUBS ENTITIES ***")
        entities = self.cb_manager.get_entities(auth=self.auth_manager, type=type)
        logger.info(f"*** RETRIEVED {len(entities)} HUB ENTITIES ***")
        return entities


def format_entities(entities: dict, hubs: dict, llegada: bool = False):
    for entity in entities:

        basehub = hubs.get(entity.get('aeropuerto')).get('id')
        date = entity.get('fecha_prevista')
        transportcode = entity.get('numero_vuelo', '')
        iataorigin = entity.get('origen', '')
        kpivalue = int(entity.get("asientos"))
        actor = entity.get('cia', '')

        entityid = f"TR_API_{basehub}"
        entitytype = "Transports"
        name = "Transporte de pasajeros."
        description = "Transporte de pasajeros por compañía."
        transportmean = "airplane"
        transporttype = "passengers-company"
        movementtype = "LLEGADA" if llegada else "SALIDA"
        basecountry = "ESPAÑA"

        date = datetime.strptime(date, '%d-%m-%Y %H:%M:%S')
        last_day = calendar.monthrange(date.year, date.month)[1]
        period_from = date.replace(day=1).isoformat(timespec='milliseconds')
        period_to = date.replace(day=last_day).isoformat(timespec='milliseconds')

        item: Dict[str, Any] = {
            'id': entityid,
            'type': entitytype,
            'calculationPeriodFrom': {'type': 'DateTime', 'value': period_from},
            'calculationPeriodTo': {'type': 'DateTime', 'value': period_to},
            'name': {'type': 'Text', 'value': name},
            'description': {'type': 'Text', 'value': description},
            'transportMean': {'type': 'Text', 'value': transportmean},
            'transportType': {'type': 'Text', 'value': transporttype},
            'baseHub': {'type': 'Text', 'value': basehub},
            'actor': {'type': 'TextUnrestricted', 'value': actor},
            'actorcountry': {'type': 'TextUnrestricted', 'value': 'NA'},
            'movementType': {'type': 'Text', 'value': movementtype},
            'transportCode': {'type': 'Text', 'value': transportcode},
            'baseCountry': {'type': 'Text', 'value': basecountry},
            'iataOrigin': {'type': 'Text', 'value': iataorigin},
            'year': {'type': 'Text', 'value': date.year},
            'month': {'type': 'Text', 'value': date.month},
            'kpiValue': {'type': 'Number', 'value': kpivalue}
        }
        yield item


if __name__ == "__main__":
    auth_manager = tc.authManager(
        endpoint=keystone,
        user=user,
        password=password,
        service=service,
        subservice=subservice
    )
    cb_manager = tc.cbManager(
        endpoint=endpoint_cb,
        timeout=timeout,
        post_retry_connect=retry_connect,
        post_retry_backoff_factor=retry_backoff_factor,
        sleep_send_batch=seconds_sleep,
        batch_size=batch_page_size
    )

    cb_service = ContextBrokerService(auth_manager, cb_manager, subservice)

    # Get Hubs entities and create Hubs dict
    hubs_cb = cb_service.get_entities(type='Hubs')
    hubs = {
        hub.get('iataCode', {}).get('value', ''): {
            'id': hub['id'], 'name': hub['name']['value']
        }
        for hub in hubs_cb
    }

    # Check airports in hubs
    missing_airports = [airport for airport in airports if airport not in hubs]
    if missing_airports:
        error_msg = f"The following airports are not in the hubs dictionary: {', '.join(missing_airports)}"
        logger.error(error_msg)
        sys.exit(-1)

    # Send entities to subserice CB
    try:
        API = APIRequest()
        all_entities = []
        for airport in airports:
            vuelos_llegada = API.get_vuelos_llegada(airport, format=lambda x: format_entities(x, hubs, llegada=True))
            vuelos_salida = API.get_vuelos_salida(airport, format=lambda x: format_entities(x, hubs))

            all_entities.extend(vuelos_llegada)
            all_entities.extend(vuelos_salida)

        if all_entities:
            cb_service.send_entities(all_entities)

        logger.info("ETL OK")
    except Exception:
        logger.error(traceback.format_exc())
        logger.info("ETL KO")
        sys.exit(-1)
4	Cuadro de mandos
Se han desarrollado los siguientes paneles en el entorno URBO para la representación visual de los indicadores del caso de uso.
4.1	Panel general del caso de uso
 
Ilustración 2. Panel general del caso de uso (visión general)
Este panel funciona como puerta de entrada al caso de uso número 12, centrado en la representación de los datos obtenidos mediante la ETL de las Apis proporcionadas por AENA.
En él, se pueden apreciar los siguientes widgets:

-	En primer lugar, un widget de tipo dato simple estructurado que enumera el total de pasajeros registrados para la fecha que se defina en el calendario.
-	A su derecha, un gráfico de barras apiladas desglosa el número de viajeros totales entre los tipos de vuelo (nacional o internacional).
-	Más a la derecha, otro widget de tipo dato simple muestra la principal ruta que opera en el aeropuerto.
-	A la derecha del todo en la primera línea, otro gráfico de barras apiladas similar al anterior, pero en este caso desglosa a los pasajeros por el tipo de movimiento (llegadas o salidas).
-	En la segunda línea, justo debajo de los anteriores, se han situado otro par de datos simples en los que figura, por un lado, el principal país del que provienen los viajeros, y por otro, la aerolínea más utilizada.
-	Por último, se ha dispuesto una serie temporal en la que se muestra el número de viajeros totales para cada uno de los meses.
4.2	Panel “mapa”
 
Ilustración 3. Panel mapa de aeropuertos
En este segundo panel, accesible solo a través de la navegación entre pestañas, se presenta información ampliada e histórica de los aeropuertos.
Para visualizar los datos del aeropuerto que el usuario desee, basta con elegirla en el selector de aeropuertos situado en la parte superior derecha del panel. Ocurre lo mismo con la fecha para la visualización de los datos relativos.
Los widgets que en él se incluyen son los siguientes:
-	Un mapa interactivo donde se muestra la ubicación del aeropuerto, y, mediante clic en el icono, se detalle el número de pasajeros correspondiente a la franja temporal definida.
-	Datos simples estructurados: con el número de pasajeros, conexiones y compañías.
-	Dos tablas. La primera con el desglose de los pasajeros por aeropuerto de destino. La segunda es idéntica, pero con las compañías.
4.3	Panel de análisis
 
Ilustración 4. Panel de análisis
Al panel de datos brutos se llega navegando a través del selector de pestañas o tabas situado arriba del todo, como en el caso anterior.
Este panel consta de: 
-	4 gráficos de barras horizontales, que distribuyen el número de pasajeros por aeropuerto, aerolínea y países; y en último lugar, las rutas por país.
-	Una serie temporal idéntica a la del panel anterior, que aporta información de contexto a los demás widgets en cuanto a la evolución de los pasajeros en el tiempo.
4.4	Panel de comparativa
 
Ilustración 5. Panel de comparativa
Este último panel tiene como finalidad la visualización de la evolución de los viajeros en el año seleccionado, y también poder comparar esta métrica para varios aeropuertos al mismo tiempo (aunque en el momento de redacción de este documento solo está disponible el análisis para el aeropuerto de Valencia), de tal modo que el aeropuerto/s y la franja temporal cargados se muestren en la serie temporal.

