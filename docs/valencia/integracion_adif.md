 






DESARROLLO DE LA INICIATIVA
CONNECTA VLCi
Expediente:  094/21-SP



Caso de uso 11. Intercambio de datos entre el ayuntamiento de Valencia-ADIF
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
2.2	Modelo DB	18
2.2.1	Scripts inicialización DB	18
3	Extracción de información de fuentes externas	24
3.1	Objetivo de la ETL del vertical de ADIF	24
3.2	Elementos necesarios	24
3.3	Método de autenticación	25
3.4	Funcionamiento	25
3.5	Ejecución del proceso de ETL	25
3.5.1	Configuración	25
3.5.2	Ejecución de la ETL principal	26
3.6	Código de la ETL	26
4	Cuadro de mandos	58
4.1	Panel general del caso de uso	58
4.2	Panel de datos históricos de las estaciones	60
4.3	Panel de datos brutos de las estaciones	61
4.4	Panel de mapa de trayectos	62

 
Índice de ilustraciones
Ilustración 1. Panel general del caso de uso (salidas)	58
Ilustración 2. Panel general del caso de uso (llegadas)	59
Ilustración 3. Panel de datos históricos de las estaciones	60
Ilustración 4. Panel de datos brutos	61
Ilustración 5. Panel de mapa de trayectos de las estaciones	62
 
1	Introducción
1.1	Objetivo
El presente documento describe los elementos desarrollados y desplegados en plataforma para la construcción del caso de uso 3, Monitorización y control de consumo de electricidad.
1.2	Destinatarios y Confidencialidad
El destinatario del presente documento es la Dirección Técnica del proyecto designada a tal efecto por Red.es y el Ayto. de València.
El acceso a este documento está restringido a la Dirección Técnica del proyecto. Cualquier acceso fuera de este ámbito, debe ser autorizado de forma expresa por la Dirección Técnica de Red.es y el Ayto. de València. 
1.3	Mantenimiento y Revisiones
Este documento es elaborado y mantenido por Telefónica.
2	Modelo de datos 
Se detalla a continuación el modelo de datos que definirá la información integrada en la plataforma, proveniente a través de procesos de extracción de la información de las apis de estaciones y circulaciones de ADIF.
2.1	Modelo NGSI
El modelo de información NGSI representa información de contexto como entidades, que tienen propiedades y se relacionan con otras entidades. Cada entidad y relación recibe una referencia única como identificador, lo que hace que los datos correspondientes se puedan exportar como conjuntos de datos de datos enlazados. Para este caso de uso, se parte de modelos estándar FIWARE, adaptados a la necesidad del proyecto.
2.1.1	Entidades principales
Entidad Caminos comerciales
Referencia de campos CommercialPath:
Atributo	ngsiType	dbType	description	example	extra	unit	range
TimeInstant	DateTime	timestamp with time zone NOT NULL	Fecha de actualización de la entidad.	"2020-03-27T07:45:00.00Z"	-	-	-
originStationCode	TextUnrestricted	text	Código de estación origen	"37200"	-	-	-
destinationStationCode	TextUnrestricted	text	Código de estación destino	"60911"	-	-	-
line	TextUnrestricted	text	Lista de lineas	["C4", "C3"]	-	-	-
launchingDate	DateTime	timestamp with time zone	Fecha de salida	"2020-03-27T07:45:00.00Z"	-	-	-
trafficType	Text	text	Tipo de tráfico	"AVLDMD"	-	-	-
operator	Text	text	Operador del trayecto	"RF"	-	-	-
product	Text	text	Producto del trayecto	"L"	-	-	-
announceableStations	TextUnrestricted	text	Lista de estaciones anunciadas	["60911"]	-	-	-
stopType	TextUnrestricted	text	Indica el tipo de paradas del trayecto	"NO_STOP"	-	-	-
announceable	TextUnrestricted	text	Indica se anuncian las paradas que realizará el trayecto	"false"	-	-	-
plannedTime	DateTime	timestamp with time zone	Fecha de salida planificada	"2020-03-27T07:45:00.00Z"	-	-	-
forecastedOrAuditedDelay	Number	double precision	Retraso del paso-lado (en segundos)	"0"	-	-	-
timeType	Text	text	Indica si el retraso del paso-lado es auditado o previsto	"FORECASTED"	-	-	-
plannedPlatform	Text	text	Via planificada. Puede ser Planificada o Planificada fiable	"1"	-	-	-
sitraPlatform	Text	text	Via de Sitra. Encapsula via Sitra auditada o Sitra prevista	"1"	-	-	-
ctcPlatform	Text	text	Via del CTC	"1"	-	-	-
operatorPlatform	Text	text	Via manual del operador	"1"	-	-	-
resultantPlatform	Text	text	Via de mayor prioridad que debe ser usada para informar	"RELIABLE_PLANNED"	-	-	-
observation	Text	text	Observacion asociada al paso-lado-camino de la respuesta		-	-	-
circulationState	Text	text	Estado de circulacion	"PENDING_TO_CIRCULATE"	-	-	-
announceState	Text	text	Estado de anuncio del paso-lado-camino de la respuesta	"NORMAL"	-	-	-
commercialProduct	TextUnrestricted	text	Tipo de producto comercial del trayecto	"NORMAL"	-	-	-
zip	Text	text	Código postal para mancomunado	"NA"	-	-	-
zone	Text	text	Zona para mancomunado	"NA"	-	-	-
district	Text	text	Distrito para mancomunado	"NA"	-	-	-
municipality	Text	text	En servicios mancomunados, indica el municipio al que pertenecen los datos. Se recomienda usar el nombre de dominio utilizado por el ayuntamiento	"NA"	-	-	-
province	Text	text	Provincia para mancomunado	"NA"	-	-	-
region	Text	text	Region/Comarca para mancomunado	"NA"	-	-	-
community	Text	text	Comunidad autónoma para mancomunado	"NA"	-	-	-
country	Text	text	País para mancomunado	"NA"	-	-	-

Ejemplo de CommercialPath (en NGSIv2):

{
    "id": "ComercialNumber-18181",
    "type": "CommercialPath",
    "TimeInstant": {
        "type": "DateTime",
        "value": "2020-03-27T07:45:00.00Z"
    },
    "originStationCode": {
        "type": "TextUnrestricted",
        "value": "37200"
    },
    "destinationStationCode": {
        "type": "TextUnrestricted",
        "value": "60911"
    },
    "line": {
        "type": "TextUnrestricted",
        "value": [
            "C4",
            "C3"
        ]
    },
    "launchingDate": {
        "type": "DateTime",
        "value": "2020-03-27T07:45:00.00Z"
    },
    "trafficType": {
        "type": "Text",
        "value": "AVLDMD"
    },
    "operator": {
        "type": "Text",
        "value": "RF"
    },
    "product": {
        "type": "Text",
        "value": "L"
    },
    "announceableStations": {
        "type": "TextUnrestricted",
        "value": [
            "60911"
        ]
    },
    "stopType": {
        "type": "TextUnrestricted",
        "value": "NO_STOP"
    },
    "announceable": {
        "type": "TextUnrestricted",
        "value": "false"
    },
    "plannedTime": {
        "type": "DateTime",
        "value": "2020-03-27T07:45:00.00Z"
    },
    "forecastedOrAuditedDelay": {
        "type": "Number",
        "value": "0"
    },
    "timeType": {
        "type": "Text",
        "value": "FORECASTED"
    },
    "plannedPlatform": {
        "type": "Text",
        "value": "1"
    },
    "sitraPlatform": {
        "type": "Text",
        "value": "1"
    },
    "ctcPlatform": {
        "type": "Text",
        "value": "1"
    },
    "operatorPlatform": {
        "type": "Text",
        "value": "1"
    },
    "resultantPlatform": {
        "type": "Text",
        "value": "RELIABLE_PLANNED"
    },
    "circulationState": {
        "type": "Text",
        "value": "PENDING_TO_CIRCULATE"
    },
    "announceState": {
        "type": "Text",
        "value": "NORMAL"
    },
    "commercialProduct": {
        "type": "TextUnrestricted",
        "value": "NORMAL"
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
    }
}

Entidad Estaciones
Referencia de campos Station:
Atributo	ngsiType	dbType	description	example	extra	unit	range
eInstant	DateTime	timestamp with time zone NOT NULL	Fecha de actualización de la entidad.	"2020-03-27T07:45:00.00Z"	-	-	-
name	TextUnrestricted	text	Nombre corto de la estación	"Val\u00e8ncia St.Isidre"	-	-	-
location	geo:json	geometry	Posición geográfica [lon, lat]	"{\"location\": { \"type\": \"geo:json\", \"value\": { \"type\": \"Point\", \"coordinates\": [-3.734511592,40.297203367]}}"	-	-	-
description	TextUnrestricted	text	Nombre descriptivo de la estación	"Val\u00e8ncia St.Isidre"	-	-	-
commercialZoneType	TextUnrestricted	text	Tipo de área comercial disponible en la estación	"NOT"	-	-	-
trafficType	TextUnrestricted	text	Tipo de tráfico	"[\"AVLDMD\",\"CERCANIAS\",\"GOODS\",\"OTHER\"]"	-	-	-
accesible	Boolean	boolean	Indica si la estación dispone de equipamiento de accesibilidad	true	-	-	-
stationServices	Boolean	boolean	Servicios adicionales de la estación	true	-	-	-
stationTransportServices	Boolean	boolean	Servicios de transporte adicionales de la estación	true	-	-	-
stationCommercialServices	Boolean	boolean	Servicios comerciales adicionales de la estación	true	-	-	-
stationActivities	Boolean	boolean	Actividades adicionales de la estación	true	-	-	-
stationType	TextUnrestricted	text	Tipo de estación	"[\"INTERNATIONAL\",\"NATIONAL\"]"	-	-	-
commuterNetwork	TextUnrestricted	text	Indica cuál es la estación que ejerce como conmutador	"VALENCIA"	-	-	-
lines	TextUnrestricted	text	Lista de lineas	"[\"C4\",\"C3\"]"	-	-	-
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

Ejemplo de Station (en NGSIv2):
{
    "id": "Station-66210",
    "type": "Station",
    "TimeInstant": {
        "type": "DateTime",
        "value": "2020-03-27T07:45:00.00Z"
    },
    "name": {
        "type": "TextUnrestricted",
        "value": "València St.Isidre"
    },
    "location": {
        "type": "geo:json",
        "value": "{\"location\": { \"type\": \"geo:json\", \"value\": { \"type\": \"Point\", \"coordinates\": [-3.734511592,40.297203367]}}"
    },
    "description": {
        "type": "TextUnrestricted",
        "value": "València St.Isidre"
    },
    "commercialZoneType": {
        "type": "TextUnrestricted",
        "value": "NOT"
    },
    "trafficType": {
        "type": "TextUnrestricted",
        "value": "[\"AVLDMD\",\"CERCANIAS\",\"GOODS\",\"OTHER\"]"
    },
    "accesible": {
        "type": "Boolean",
        "value": true
    },
    "stationServices": {
        "type": "Boolean",
        "value": true
    },
    "stationTransportServices": {
        "type": "Boolean",
        "value": true
    },
    "stationCommercialServices": {
        "type": "Boolean",
        "value": true
    },
    "stationActivities": {
        "type": "Boolean",
        "value": true
    },
    "stationType": {
        "type": "TextUnrestricted",
        "value": "[\"INTERNATIONAL\",\"NATIONAL\"]"
    },
    "commuterNetwork": {
        "type": "TextUnrestricted",
        "value": "VALENCIA"
    },
    "lines": {
        "type": "TextUnrestricted",
        "value": "[\"C4\",\"C3\"]"
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
    }
}

2.2	Modelo DB
Para la persistencia de los datos del caso de uso se han desplegado las siguientes tablas en el datasource PostgreSQL:
Ficheros SQL asociados al modelo CommercialPath
•	commercialpath_historic.sql: Fichero SQL del flujo historic (tipo FLOW_HISTORIC) en modelo CommercialPath
•	commercialpath_lastdata.sql: Fichero SQL del flujo lastdata (tipo FLOW_LASTDATA) en modelo CommercialPath
Ficheros SQL asociados al modelo Station
•	station_historic.sql: Fichero SQL del flujo historic (tipo FLOW_HISTORIC) en modelo Station
•	station_lastdata.sql: Fichero SQL del flujo lastdata (tipo FLOW_LASTDATA) en modelo Station
Además, se han incorporado las siguientes vistas:
•	commercialpath_jn.sql: Fichero SQL del flujo jn (tipo FLOW_JOIN_VIEW)
•	station_jn.sql: Fichero SQL del flujo jn (tipo FLOW_JOIN_VIEW)

2.2.1	Scripts inicialización DB

CREATE TABLE IF NOT EXISTS :target_schema.adif_commercialpath (
  timeinstant timestamp with time zone NOT NULL,
  originstationcode text,
  destinationstationcode text,
  line text,
  launchingdate timestamp with time zone,
  traffictype text,
  operator text,
  product text,
  announceablestations text,
  stoptype text,
  announceable text,
  plannedtime timestamp with time zone,
  forecastedorauditeddelay double precision,
  timetype text,
  plannedplatform text,
  sitraplatform text,
  ctcplatform text,
  operatorplatform text,
  resultantplatform text,
  observation text,
  circulationstate text,
  announcestate text,
  commercialproduct text,
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT adif_commercialpath_pkey PRIMARY KEY (timeinstant, entityid)
);

CREATE OR REPLACE VIEW :target_schema.adif_commercialpath_jn AS
  SELECT
    left_table.timeinstant,
    left_table.originstationcode,
    left_table.destinationstationcode,
    left_table.line,
    left_table.launchingdate,
    left_table.traffictype,
    left_table.operator,
    left_table.product,
    left_table.announceablestations,
    left_table.stoptype,
    left_table.announceable,
    left_table.plannedtime,
    left_table.forecastedorauditeddelay,
    left_table.timetype,
    left_table.plannedplatform,
    left_table.sitraplatform,
    left_table.ctcplatform,
    left_table.operatorplatform,
    left_table.resultantplatform,
    left_table.observation,
    left_table.circulationstate,
    left_table.announcestate,
    left_table.entityid,
    left_table.entitytype,
    left_table.recvtime,
    left_table.fiwareservicepath,
    left_table.commercialProduct,
    right_table.municipality,
    right_table.zip,
    right_table.zone,
    right_table.district,
    right_table.province,
    right_table.region,
    right_table.community,
    right_table.country
  FROM
    :target_schema.adif_commercialpath left_table
  INNER JOIN
    :target_schema.adif_commercialpath_lastdata right_table
  ON left_table.entityid = right_table.entityid
;

CREATE TABLE IF NOT EXISTS :target_schema.adif_commercialpath_lastdata (
  timeinstant timestamp with time zone NOT NULL,
  originstationcode text,
  destinationstationcode text,
  line text,
  launchingdate timestamp with time zone,
  traffictype text,
  operator text,
  product text,
  announceablestations text,
  stoptype text,
  announceable text,
  plannedtime timestamp with time zone,
  forecastedorauditeddelay double precision,
  timetype text,
  plannedplatform text,
  sitraplatform text,
  ctcplatform text,
  operatorplatform text,
  resultantplatform text,
  observation text,
  circulationstate text,
  announcestate text,
  commercialproduct text,
  zip text,
  zone text,
  district text,
  municipality text,
  province text,
  region text,
  community text,
  country text,
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT adif_commercialpath_lastdata_pkey PRIMARY KEY (entityid)
);
CREATE TABLE IF NOT EXISTS :target_schema.adif_station (
  timeinstant timestamp with time zone NOT NULL,
  traffictype text,
  accesible boolean,
  stationservices boolean,
  stationtransportservices boolean,
  stationcommercialservices boolean,
  stationactivities boolean,
  stationtype text,
  commuternetwork text,
  lines text,
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT adif_station_pkey PRIMARY KEY (timeinstant, entityid)
);

CREATE OR REPLACE VIEW :target_schema.adif_station_jn AS
  SELECT
    left_table.timeinstant,
    left_table.traffictype,
    left_table.stationtype,
    left_table.commuternetwork,
    left_table.lines,
    left_table.entityid,
    left_table.entitytype,
    left_table.recvtime,
    left_table.fiwareservicepath,
    left_table.accesible,
    left_table.stationServices,
    left_table.stationTransportServices,
    left_table.stationCommercialServices,
    left_table.stationActivities,
    right_table.name,
    right_table.location,
    right_table.description,
    right_table.commercialZoneType,
    right_table.municipality,
    right_table.zip,
    right_table.zone,
    right_table.district,
    right_table.province,
    right_table.region,
    right_table.community,
    right_table.country
  FROM
    :target_schema.adif_station left_table
  INNER JOIN
    :target_schema.adif_station_lastdata right_table
  ON left_table.entityid = right_table.entityid
;

CREATE TABLE IF NOT EXISTS :target_schema.adif_station_lastdata (
  timeinstant timestamp with time zone NOT NULL,
  name text,
  location geometry,
  description text,
  commercialzonetype text,
  traffictype text,
  accesible boolean,
  stationservices boolean,
  stationtransportservices boolean,
  stationcommercialservices boolean,
  stationactivities boolean,
  stationtype text,
  commuternetwork text,
  lines text,
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
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT adif_station_lastdata_pkey PRIMARY KEY (entityid)
);
3	Extracción de información de fuentes externas
La fuente de información y base para este caso de uso son las API de ADIF (estaciones, salidas y circulaciones):
•	Station: https://estaciones.api.adif.es/portroyalmanager/stations/allstations/reducedinfo/
•	Arrivals: https://circulacion.api.adif.es/portroyalmanager/circulationpaths/arrivals/traffictype/
•	Deapertures: https://circulacion.api.adif.es/portroyalmanager/circulationpaths/deapertures/traffictype/
3.1	Objetivo de la ETL del vertical de ADIF
El objetivo de esta ETL es procesar y cargar en el Context Broker los datos de estaciones de trenes referentes a Valencia. Actualmente, se dispone de un proceso adaptado a los datos obtenidos de las apis de estaciones y circulaciones
3.2	Elementos necesarios
Software
Para poder ejecutar el proceso de ETL en un equipo es necesario disponer del siguiente software:
Software	Versión
Python	>= 3.8
pip	*
Dentro del directorio con el código del proceso (etls) se encuentra un fichero requirements.txt que contiene las dependencias que necesita el script para funcionar adecuadamente. Es recomendable crear un entorno virtual de Python (virtualenv), acceder al mismo e instalar dichas dependencias mediante el comando pip install -r requirements.txt (recuerde ubicarse en su shell dentro del directorio etls).
Ejemplo:
  $ cd PRO/assets/etl
  $ virtualenv venv
  $ source venv/bin/activate
  (venv)$ pip install -r requirements.txt
3.3	Método de autenticación
Se envía en los headers de cada petición una API KEY que viene especificada en las variables de entorno:
headers = {'User-key': “API_KEY”}
3.4	Funcionamiento
La ETL está programada para ser ejecutada todos los días, y los datos obtenidos están relacionados a estaciones, llegada de estaciones y salida de estaciones que son adaptados y transformados al modelo de Station y CommercialPath respectivamente.
•	Se hace una petición a la URL Station para obtener la información de todas las estaciones y ser adaptada al modelo de Station.
•	Las estaciones retornadas en el punto anterior son comparadas con un listado de estaciones que se ingresan al ser ejecutada la ETL y se procesan para obtener la información de salidas y llegadas de cada estación, en este paso se usan las URL’s de arrivals y deapertures para ser adaptadas al modelo de CommercialPath.
3.5	Ejecución del proceso de ETL
3.5.1	Configuración
La información obtenida de la API de estaciones se almacena en forma de entidades tipo WeatherObserved en el Context Broker del cliente. La aplicación necesita de las siguientes variables de entorno para conectar al context broker y autenticarse:
•	ETL_ADIF_ENVIRONMENT_ENDPOINT_KEYSTONE: URL de conexión al keystone, por ejemplo Url de autenticación con el CB
•	ETL_ADIF_ENVIRONMENT_ENDPOINT_CB: URL de conexión a Orion, por ejemplo Url del CB
•	ETL_ADIF_ENVIRONMENT_SERVICE: Nombre del servicio.
•	ETL_ADIF_ENVIRONMENT_SUBSERVICE: Nombre del subservicio.
•	ETL_ADIF_ENVIRONMENT_USER: Usuario de plataforma con permiso para escribir entidades en el servicio y subservicio especificado.
•	ETL_ADIF_ENVIRONMENT_PASSWORD: Contraseña del usuario especificado.
•	ETL_ADIF_URL_STATION: Url del api de ADIF para obtener las estaciones. Por ejemplo https://estaciones.api.adif.es/portroyalmanager/stations/allstations/reducedinfo/
•	ETL_ADIF_URL_CIRCULACION:Url del api de ADIF para obtener los caminos. Por ejemplo https://circulacion.api.adif.es/portroyalmanager/circulationpaths/arrivals/traffictype/
•	ETL_ADIF_API_STATION:API key para acceder a la url de las estaciones de ADIF.
•	ETL_ADIF_STATION_CODES:Lista de código de estaciones de las cuales se requiere obtener los datos.Ejemplo 03216,55017.
•	ETL_ADIF_API_CIRCULACION:API key para acceder a la url de los caminos de ADIF.
•	ETL_ADIF_SETTINGS_BATCH_PAGE_SIZE: Tamaño de batch a ser enviado. Por ejemplo 80.
•	ETL_ADIF_SETTINGS_SLEEP_SEND_BATCH: Tiempo de espera entre envios de batch. Por ejemplo 5 en segundos.
•	ETL_ADIF_SETTINGS_SLEEP_CALL_API: Tiempo de espera entre llamadas a la API. Por ejemplo 60 en segundos.
•	ETL_ADIF_SETTINGS_TIMEOUT: Tiempo de espera para obtener una respuesta de los endpoint. Por ejemplo 10 en segundos.
•	ETL_ADIF_SETTINGS_POST_RETRY_CONNECT: Cantidad de reintentos de invocaciones a endpoint. Por ejemplo 3.
•	ETL_ADIF_SETTINGS_POST_RETRY_BACKOFF_FACTOR: Factor de espera entre cada reintento. Por ejemplo 20 en segundos.
•	ETL_ADIF_SETTINGS_IS_DEBUG: Indica si el logger está en modo debug.
•	ETL_LOG_LEVEL: Indica el nivel del logger.
Estas variables pueden estar definidas en el entorno, o pueden almacenarse en un fichero env.json en el mismo directorio que la ETL.
3.5.2	Ejecución de la ETL principal
Nota: en caso de disponer de un entorno virtual de Python, es necesario activarlo para ejecutar el script con las dependencias instaladas en dicho entorno.
Para iniciar el proceso se ha de ejecutar el comando python etl.py, (recuerde situar su shell dentro del directorio ADIF-vertical/etls). También puede ejecutar el script desde cualquier otro directorio, pero tendrá que indicar la ruta absoluta o relativa al script.
3.6	Código de la ETL
El código de la ETL se describe a continuación:
# Copyright 2022 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
# PROJECT: LaPalma-project
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
# pylint:disable=invalid-name,line-too-long,missing-function-docstring,too-many-locals

"""ADIF ETL"""
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from typing import List, Any

import requests
import tc_etl_lib as tc
import urllib3
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

urllib3.disable_warnings()
load_dotenv()
# Get the config variables
endpoint_cb = os.getenv('ETL_ADIF_ENVIRONMENT_ENDPOINT_CB', '')
endpoint_keystone = os.getenv('ETL_ADIF_ENVIRONMENT_ENDPOINT_KEYSTONE', '')
service = os.getenv('ETL_ADIF_ENVIRONMENT_SERVICE', '')
subservice_cfg = os.getenv('ETL_ADIF_ENVIRONMENT_SUBSERVICE', '')
user = os.getenv('ETL_ADIF_ENVIRONMENT_USER', '')
password = os.getenv('ETL_ADIF_ENVIRONMENT_PASSWORD', '')
api_station = os.getenv('ETL_ADIF_API_STATION', '')
api_circulacion = os.getenv('ETL_ADIF_API_CIRCULACION')
url_station = os.getenv('ETL_ADIF_URL_STATION', '')
url_circulacion_arrivals = os.getenv('ETL_ADIF_URL_CIRCULACION_ARRIVALS', '')
url_circulacion_departures = os.getenv('ETL_ADIF_URL_CIRCULACION_DEPARTURES', '')
station_codes_arrivals = os.getenv('ETL_ADIF_STATION_CODES_ARRIVALS', '')
station_codes_departures = os.getenv('ETL_ADIF_STATION_CODES_DEPARTURES', '')
post_retry_connect = int(os.getenv('ETL_ADIF_SETTINGS_POST_RETRY_CONNECT', '3'))
post_retry_backoff_factor = float(os.getenv("ETL_ADIF_SETTINGS_POST_RETRY_BACKOFF_FACTOR", '2'))
timeout = int(os.getenv('ETL_ADIF_SETTINGS_TIMEOUT', '10'))
sleep_send_batch = int(os.getenv("ETL_ADIF_SETTINGS_SLEEP_SEND_BATCH", '5'))
batch_size = int(os.getenv("ETL_ADIF_SETTINGS_BATCH_PAGE_SIZE", '20'))
sleep_call_api = int(os.getenv("ETL_ADIF_SETTINGS_SLEEP_CALL_API", '60'))
is_debug = os.getenv('ETL_ADIF_SETTINGS_IS_DEBUG', '')
logLevelString = os.getenv('ETL_LOG_LEVEL', 'DEBUG')
date_format = "%Y-%m-%d"
datetime_format_cb = "%Y-%m-%dT%H:%M:00.00Z"

# sets the logging configuration
logging.basicConfig(
    level=logLevelString,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-ADIF-integration | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
# get logger to use it
logger = logging.getLogger()
normalizer = tc.normalizer()


def parse_value(value, default_value=''):
    """
    Checks if the provided value is None or empty, and returns a default value if it is.
    This function solves issue #29

    Parameters:
    value (Any): The value to be checked. This can be of any data type.
    default_value (Any): The default value to return if the original value is None or empty. 
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return default_value
    return value


def get_station_data(session: requests.Session, cbm: tc.cb.cbManager, auth: tc.auth.authManager) -> List[Any]:
    headers = {
        'User-key': api_station,
        'Accept': 'application/json;charset=UTF-8'
    }

    url = '/'.join((url_station.strip('/'), api_station)) + '/'
    logger.info(url)
    result = session.get(url=url, headers=headers)
    if result.status_code < 200 or result.status_code >= 300:
        raise ValueError(f"Failed to query URL {url}: {result.text}")
    resp = result.json()
    timeinstant = datetime.now().isoformat(timespec='milliseconds')
    entities = []

    for reg in resp['requestedStationInfoList']:
        station = {
            "id": "ENT-STATION-" + reg['stationCode'],
            "type": "Station",
            "TimeInstant": {
                "type": "DateTime",
                "value": timeinstant
            },
            "location": {
                "type": "geo:json",
                "value": {
                    "type": "Point",
                    "coordinates": [
                        reg['stationInfo']['location']['longitude'],
                        reg['stationInfo']['location']['latitude']
                    ]
                }
            },
            "name": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['shortName']
            },
            "description": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['shortName']
            },
            "stationType": {
                "type": "Text",
                "value": reg['stationInfo']['stationType']
            },
            "trafficType": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['trafficType']
            },
            "commercialZoneType": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['commercialZoneType']
            },

            "lines": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['lines']
            }
        }
        if reg['stationInfo'] is not None and reg['stationInfo']['commuterNetwork'] is not None:
            reg['commuterNetwork'] = {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['commuterNetwork']
            }
        if reg['stationInfo'] is not None and reg['stationInfo']['accessible'] is not None and reg['stationInfo']['accessible']['accessible'] is not None:
            station["accesible"] = {
                "type": "Boolean",
                "value": reg['stationInfo']['accessible']['accessible']
            }
        else:
            station["accesible"] = {
                "type": "None",
                "value": None
            }
        if reg['stationServices'] is not None:
            station["stationServices"] = {
                "type": "Boolean",
                "value": reg['stationServices']
            }
        else:
            station["stationServices"] = {
                "type": "None",
                "value": None
            }
        if reg['stationTransportServices'] is not None:
            station["stationTransportServices"] = {
                "type": "Boolean",
                "value": reg['stationTransportServices']
            }
        else:
            station["stationTransportServices"] = {
                "type": "None",
                "value": None
            }
        if reg['stationCommercialServices'] is not None :
            station["stationCommercialServices"] = {
                "type": "Boolean",
                "value": reg['stationCommercialServices']
            }
        else:
            station["stationCommercialServices"] = {
                "type": "None",
                "value": None
            }
        if reg['stationActivities'] is not None :
            station["stationActivities"] = {
                "type": "Boolean",
                "value": reg['stationActivities']
            }
        else:
            station["stationActivities"] = {
                "type": "None",
                "value": None
            }
        entities.append(station)

    stations_arrivals = [x.strip() for x in station_codes_arrivals.split(',')]
    stations_departures = [x.strip() for x in station_codes_departures.split(',')]
    respList = [x for x in list(resp['requestedStationInfoList']) if x['stationCode'] in stations_arrivals]
    for station in respList:
        urlCommercialPath = url_circulacion_arrivals  # '/'.join((url_circulacion.strip('/'), 'portroyalmanager/circulationpaths/departures/traffictype'))
        headersCommercialPath = {
            'User-key': api_circulacion,
            'Accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json;charset=UTF-8'
        }
        body = {
            'page': {
                'pageNumber': '1'
            },
            'commercialService': 'BOTH',
            'commercialStopType': 'BOTH',
            'stationCode': station['stationCode'],
            'trafficType': 'ALL'
        }
        logger.info(headersCommercialPath)
        logger.info(body)
        # FIXME: The API is rate limited. We wait a second between requests.
        # There might be a better way to rate-limit this
        time.sleep(sleep_call_api)  # pedido de tener 1 minuto de espera entre llamadas de Api´s
        respComercialPath = session.post(url=urlCommercialPath, headers=headersCommercialPath, json=body)
        if respComercialPath.status_code == 204:
            logger.info("Response to %s was empty", urlCommercialPath)
            commPathBody = {}
        elif respComercialPath.status_code == 200:
            commPathBody = respComercialPath.json()
        else:
            raise ValueError(
                f"Failed to query URL {urlCommercialPath}: [{respComercialPath.status_code}] {respComercialPath.text}")
        for com in commPathBody.get('commercialPaths', []):
            origin_station_name = cbm.get_entities(auth=auth,type='Station', id='ENT-STATION-' + com['commercialPathInfo']['commercialPathKey']['originStationCode'], service=service, subservice=subservice_cfg)
            destination_station_name = cbm.get_entities(auth=auth,type='Station', id='ENT-STATION-' + com['commercialPathInfo']['commercialPathKey']['destinationStationCode'], service=service, subservice=subservice_cfg)

            commercialPath = {
                "id": "ENT-COMMERCIALPATH-" +
                      com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey'][
                          'commercialNumber'],
                "type": "CommercialPath",
                "TimeInstant": {
                    "type": "DateTime",
                    "value": timeinstant
                },
                "originStationCode": {
                    "type": "TextUnrestricted",
                    "value": origin_station_name[0]['name']
                },
                "destinationStationCode": {
                    "type": "TextUnrestricted",
                    "value": destination_station_name[0]['name']
                },
                "line": {
                    "type": "TextUnrestricted",
                    "value": com['commercialPathInfo']['line']
                },
                "trafficType": {
                    "type": "Text",
                    "value": com['commercialPathInfo']['trafficType']
                },
                "operator": {
                    "type": "Text",
                    "value": com['commercialPathInfo']['opeProComPro']['operator']
                },
                "product": {
                    "type": "Text",
                    "value": com['commercialPathInfo']['opeProComPro']['product']
                },
                "commercialProduct": {
                    "type": "TextUnrestricted",
                    "value": com['commercialPathInfo']['opeProComPro']['commercialProduct']
                },
                "announceableStations": {
                    "type": "TextUnrestricted",
                    "value": com['commercialPathInfo']['announceableStations'][0]
                }
            }
            if com['commercialPathInfo'] is not None and com['commercialPathInfo']['commercialPathKey'] is not None and com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey'] is not None and com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey']['launchingDate'] is not None:
                launchDate = float(com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey']['launchingDate'])
                commercialPath["launchingDate"] = {
                    "type": "DateTime",
                    "value": datetime.fromtimestamp(launchDate/1000).strftime(datetime_format_cb)
                }
            if com['passthroughStep'] is not None:
                if com['passthroughStep']['stopType'] is not None:
                    commercialPath["stopType"] = {
                        "type": "TextUnrestricted",
                        "value": com['passthroughStep']['stopType']
                    }
                if com['passthroughStep']['stopType'] is not None:
                    commercialPath["stopType"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['stopType']
                    }
                if com['passthroughStep']['arrivalPassthroughStepSides'] is not None and com['passthroughStep']['arrivalPassthroughStepSides']['plannedTime'] is not None:
                    plannedTime = float(com['passthroughStep']['arrivalPassthroughStepSides']['plannedTime'])
                    commercialPath["plannedTime"] = {
                        "type": "DateTime",
                        "value": datetime.fromtimestamp(plannedTime/1000).strftime(datetime_format_cb)
                    }
                if com['passthroughStep']['announceable'] is not None:
                    commercialPath["announceable"] = {
                        "type": "Boolean",
                        "value": com['passthroughStep']['announceable']
                    }
                if com['passthroughStep']['arrivalPassthroughStepSides'] is not None:
                    commercialPath["forecastedOrAuditedDelay"] = {
                        "type": "Number",
                        "value": int(com['passthroughStep']['arrivalPassthroughStepSides']['forecastedOrAuditedDelay'])
                    }
                    commercialPath["timeType"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['timeType']
                    }
                    commercialPath["plannedPlatform"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['plannedPlatform']
                    }
                    commercialPath["sitraPlatform"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['sitraPlatform']
                    }
                    commercialPath["ctcPlatform"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['ctcPlatform']
                    }
                    commercialPath["operatorPlatform"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['operatorPlatform']
                    }
                    commercialPath["resultantPlatform"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['resultantPlatform']
                    }
                    commercialPath["observation"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['observation']
                    }
                    commercialPath["circulationState"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['circulationState']
                    }
                    commercialPath["announceState"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['arrivalPassthroughStepSides']['announceState']
                    }
                entities.append(commercialPath)
    # Stations departures
    resp_list_departures = [x for x in list(resp['requestedStationInfoList']) if x['stationCode'] in stations_departures]
    logger.info('Departures stations: %s', resp_list_departures)
    for station in resp_list_departures:
        url_commercial_path_departures = url_circulacion_departures
        headers_commercial_path = {
            'User-key': api_circulacion,
            'Accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json;charset=UTF-8'
        }
        body = {
            'page': {
                'pageNumber': '1'
            },
            'commercialService': 'BOTH',
            'commercialStopType': 'BOTH',
            'stationCode': station['stationCode'],
            'trafficType': 'ALL'
        }
        # FIXME: The API is rate limited. We wait a second between requests.
        # There might be a better way to rate-limit this
        time.sleep(sleep_call_api)  # pedido de tener 1 minuto de espera entre llamadas de Api´s
        respComercialPath = session.post(url=url_commercial_path_departures, headers=headersCommercialPath, json=body)
        if respComercialPath.status_code == 204:
            logger.info("Response to %s was empty", url_commercial_path_departures)
            commPathBody = {}
        elif respComercialPath.status_code == 200:
            commPathBody = respComercialPath.json()
        logger.info(headers_commercial_path)
        logger.info(body)
        logger.info(commPathBody)

    return entities


class TimeoutHTTPAdapter(HTTPAdapter):
    """HTTP Adapter for global timeouts. See https://github.com/psf/requests/issues/2011"""

    def __init__(self, timeout=None, api_req_per_sec=None, api_burst=None, **kwargs):
        """Add support for timeout variable"""
        super().__init__(**kwargs)
        self.__timeout = timeout
        self.__api_req_per_sec = api_req_per_sec
        self.__tokens = api_burst or 0
        self.__last = time.time()

    def send(self, *args, **kwargs):
        """Inject timeout into request"""
        if self.__api_req_per_sec:
            # Rate-limit requests
            while self.__tokens < 1:
                # Add as many tokens as needed, considering the token rate
                # and the last time we updated the token count
                last = time.time()
                self.__tokens += max(last - self.__last, 0) * self.__api_req_per_sec
                self.__last = last
                # If there are burst tokens left, consume them
                if self.__tokens < 1:
                    sleep_time = (1 - self.__tokens) / self.__api_req_per_sec
                    time.sleep(sleep_time)
            self.__tokens -= 1
        kwargs['timeout'] = self.__timeout
        return super().send(*args, **kwargs)


def get_stations(session: requests.Session) -> List[Any]:
    headers = {
        'User-key': api_station,
        'Accept': 'application/json;charset=UTF-8'
    }

    url = '/'.join((url_station.strip('/'), api_station)) + '/'
    logger.info(url)
    result = session.get(url=url, headers=headers)
    if result.status_code < 200 or result.status_code >= 300:
        raise ValueError(f"Failed to query URL {url}: {result.text}")
    resp = result.json()
    timeinstant = datetime.now().isoformat(timespec='milliseconds')
    entities = []

    for reg in resp['requestedStationInfoList']:
        station = {
            "id": "ENT-STATION-" + reg['stationCode'],
            "type": "Station",
            "TimeInstant": {
                "type": "DateTime",
                "value": timeinstant
            },
            "location": {
                "type": "geo:json",
                "value": {
                    "type": "Point",
                    "coordinates": [
                        reg['stationInfo']['location']['longitude'],
                        reg['stationInfo']['location']['latitude']
                    ]
                }
            },
            "name": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['shortName']
            },
            "description": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['shortName']
            },
            "stationType": {
                "type": "Text",
                "value": reg['stationInfo']['stationType']
            },
            "trafficType": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['trafficType']
            },
            "commercialZoneType": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['commercialZoneType']
            },

            "lines": {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['lines']
            }
        }
        if reg['stationInfo'] is not None and reg['stationInfo']['commuterNetwork'] is not None:
            reg['commuterNetwork'] = {
                "type": "TextUnrestricted",
                "value": reg['stationInfo']['commuterNetwork']
            }
        if reg['stationInfo'] is not None and reg['stationInfo']['accessible'] is not None and \
                reg['stationInfo']['accessible']['accessible'] is not None:
            station["accesible"] = {
                "type": "Boolean",
                "value": reg['stationInfo']['accessible']['accessible']
            }
        else:
            station["accesible"] = {
                "type": "None",
                "value": None
            }
        if reg['stationServices'] is not None:
            station["stationServices"] = {
                "type": "Boolean",
                "value": reg['stationServices']
            }
        else:
            station["stationServices"] = {
                "type": "None",
                "value": None
            }
        if reg['stationTransportServices'] is not None:
            station["stationTransportServices"] = {
                "type": "Boolean",
                "value": reg['stationTransportServices']
            }
        else:
            station["stationTransportServices"] = {
                "type": "None",
                "value": None
            }
        if reg['stationCommercialServices'] is not None:
            station["stationCommercialServices"] = {
                "type": "Boolean",
                "value": reg['stationCommercialServices']
            }
        else:
            station["stationCommercialServices"] = {
                "type": "None",
                "value": None
            }
        if reg['stationActivities'] is not None:
            station["stationActivities"] = {
                "type": "Boolean",
                "value": reg['stationActivities']
            }
        else:
            station["stationActivities"] = {
                "type": "None",
                "value": None
            }
        entities.append(station)
    return entities


def get_arrivals(session: requests.Session, stations: List[Any], cbm: tc.cb.cbManager, auth:tc.auth.authManager) -> List[Any]: # noqa
    stations_arrivals = [x.strip() for x in station_codes_arrivals.split(',')]
    respList = [
        str(x['id']).split('ENT-STATION-')[1]
        for x in stations if str(x['id']).split('ENT-STATION-')[1] in stations_arrivals
    ]
    logger.info('Arrivals stations: %s', respList)
    timeinstant = datetime.now().isoformat(timespec='milliseconds')
    entities = []
    for station in respList:
        urlCommercialPath = url_circulacion_arrivals  # noqa '/'.join((url_circulacion.strip('/'), 'portroyalmanager/circulationpaths/departures/traffictype'))
        headersCommercialPath = {
            'User-key': api_circulacion,
            'Accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json;charset=UTF-8'
        }
        body = {
            'page': {
                'pageNumber': '1'
            },
            'commercialService': 'BOTH',
            'commercialStopType': 'BOTH',
            'stationCode': station,
            'trafficType': 'ALL'
        }
        time.sleep(sleep_call_api)  # pedido de tener 1 minuto de espera entre llamadas de Api´s
        respComercialPath = session.post(url=urlCommercialPath, headers=headersCommercialPath, json=body)
        if respComercialPath.status_code == 204:
            logger.info("Response to %s was empty", urlCommercialPath)
            commPathBody = {}
        elif respComercialPath.status_code == 200:
            commPathBody = respComercialPath.json()
        else:
            raise ValueError(
                f"Failed to query URL {urlCommercialPath}: [{respComercialPath.status_code}] {respComercialPath.text}")
        logger.info(commPathBody)
        for com in commPathBody.get('commercialPaths', []):
            origin_station_name = cbm.get_entities(auth=auth, type='Station',
                                                   id='ENT-STATION-' + com['commercialPathInfo']['commercialPathKey'][
                                                       'originStationCode'], service=service, subservice=subservice_cfg)
            destination_station_name = cbm.get_entities(auth=auth, type='Station', id='ENT-STATION-' +
                                                                                      com['commercialPathInfo'][
                                                                                          'commercialPathKey'][
                                                                                          'destinationStationCode'],
                                                        service=service, subservice=subservice_cfg)

            commercialPath = {
                "id": "ENT-COMMERCIALPATH-" + com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey']['commercialNumber'],
                "type": "CommercialPath",
                "TimeInstant": {
                    "type": "DateTime",
                    "value": timeinstant
                },
                "originStationCode": {
                    "type": "TextUnrestricted",
                    "value": parse_value(origin_station_name[0]['name']['value'])
                },
                "destinationStationCode": {
                    "type": "TextUnrestricted",
                    "value": parse_value(destination_station_name[0]['name']['value'])
                },
                "line": {
                    "type": "TextUnrestricted",
                    "value": parse_value(com['commercialPathInfo']['line'])
                },
                "trafficType": {
                    "type": "Text",
                    "value": parse_value(com['commercialPathInfo']['trafficType'])
                },
                "operator": {
                    "type": "Text",
                    "value": parse_value(com['commercialPathInfo']['opeProComPro']['operator'])
                },
                "product": {
                    "type": "Text",
                    "value": parse_value(com['commercialPathInfo']['opeProComPro']['product'])
                },
                "commercialProduct": {
                    "type": "TextUnrestricted",
                    "value": parse_value(com['commercialPathInfo']['opeProComPro']['commercialProduct'])
                },
                "announceableStations": {
                    "type": "TextUnrestricted",
                    "value": parse_value(com['commercialPathInfo']['announceableStations'][0])
                }
            }
            if com['commercialPathInfo'] is not None and com['commercialPathInfo']['commercialPathKey'] is not None and \
                    com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey'] is not None and \
                    com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey'][
                        'launchingDate'] is not None:
                launchDate = float(
                    com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey']['launchingDate'])
                commercialPath["launchingDate"] = {
                    "type": "DateTime",
                    "value": datetime.fromtimestamp(launchDate / 1000).strftime(datetime_format_cb)
                }
            if com['passthroughStep'] is not None:
                if com['passthroughStep']['stopType'] is not None:
                    commercialPath["stopType"] = {
                        "type": "TextUnrestricted",
                        "value": com['passthroughStep']['stopType']
                    }
                if com['passthroughStep']['stopType'] is not None:
                    commercialPath["stopType"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['stopType']
                    }
                if com['passthroughStep']['arrivalPassthroughStepSides'] is not None and \
                        com['passthroughStep']['arrivalPassthroughStepSides']['plannedTime'] is not None:
                    plannedTime = float(com['passthroughStep']['arrivalPassthroughStepSides']['plannedTime'])
                    commercialPath["plannedTime"] = {
                        "type": "DateTime",
                        "value": datetime.fromtimestamp(plannedTime / 1000).strftime(datetime_format_cb)
                    }
                if com['passthroughStep']['announceable'] is not None:
                    commercialPath["announceable"] = {
                        "type": "Boolean",
                        "value": com['passthroughStep']['announceable']
                    }
                if com['passthroughStep']['arrivalPassthroughStepSides'] is not None:
                    commercialPath["forecastedOrAuditedDelay"] = {
                        "type": "Number",
                        "value": int(com['passthroughStep']['arrivalPassthroughStepSides']['forecastedOrAuditedDelay'])
                    }
                    commercialPath["timeType"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['timeType'])
                    }
                    commercialPath["plannedPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['plannedPlatform'])
                    }
                    commercialPath["sitraPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['sitraPlatform'])
                    }
                    commercialPath["ctcPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['ctcPlatform'])
                    }
                    commercialPath["operatorPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['operatorPlatform'])
                    }
                    commercialPath["resultantPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['resultantPlatform'])
                    }
                    commercialPath["observation"] = {
                        "type": "Text",
                        "value": normalizer(
                            parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['observation'])
                        )
                    }
                    commercialPath["circulationState"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['circulationState'])
                    }
                    commercialPath["announceState"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['arrivalPassthroughStepSides']['announceState'])
                    }
                entities.append(commercialPath)
        return entities


def get_departures(session: requests.Session, stations: List[Any], cbm: tc.cb.cbManager, auth: tc.auth.authManager) -> List[Any]:
    stations_departures = [x.strip() for x in station_codes_departures.split(',')]
    respList = [str(x['id']).split('ENT-STATION-')[1] for x in stations if str(x['id']).split('ENT-STATION-')[1] in stations_departures]
    logger.info('Departures stations: %s', respList)
    timeinstant = datetime.now().isoformat(timespec='milliseconds')
    entities = []
    for station in respList:
        urlCommercialPath = url_circulacion_departures  # '/'.join((url_circulacion.strip('/'), 'portroyalmanager/circulationpaths/departures/traffictype'))
        headersCommercialPath = {
            'User-key': api_circulacion,
            'Accept': 'application/json;charset=UTF-8',
            'Content-Type': 'application/json;charset=UTF-8'
        }
        body = {
            'page': {
                'pageNumber': '1'
            },
            'commercialService': 'BOTH',
            'commercialStopType': 'BOTH',
            'stationCode': station,
            'trafficType': 'ALL'
        }
        time.sleep(sleep_call_api)  # pedido de tener 1 minuto de espera entre llamadas de Api´s
        respComercialPath = session.post(url=urlCommercialPath, headers=headersCommercialPath, json=body)
        if respComercialPath.status_code == 204:
            logger.info("Response to %s was empty", urlCommercialPath)
            commPathBody = {}
        elif respComercialPath.status_code == 200:
            commPathBody = respComercialPath.json()
        else:
            raise ValueError(
                f"Failed to query URL {urlCommercialPath}: [{respComercialPath.status_code}] {respComercialPath.text}")
        logger.info(commPathBody)
        for com in commPathBody.get('commercialPaths', []):
            origin_station_name = cbm.get_entities(auth=auth, type='Station',
                                                   id='ENT-STATION-' + com['commercialPathInfo']['commercialPathKey'][
                                                       'originStationCode'], service=service, subservice=subservice_cfg)
            destination_station_name = cbm.get_entities(auth=auth, type='Station', id='ENT-STATION-' +
                                                                                      com['commercialPathInfo'][
                                                                                          'commercialPathKey'][
                                                                                          'destinationStationCode'],
                                                        service=service, subservice=subservice_cfg)
            if origin_station_name.__len__()<1:
                logger.info('No name for station %s',com['commercialPathInfo']['commercialPathKey']['originStationCode'])
                continue
            if destination_station_name.__len__()<1:
                logger.info('No name for station %s',com['commercialPathInfo']['commercialPathKey']['destinationStationCode'])
                continue

            commercialPath = {
                "id": "ENT-COMMERCIALPATH-" + com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey']['commercialNumber'], # noqa
                "type": "CommercialPath",
                "TimeInstant": {
                    "type": "DateTime",
                    "value": timeinstant
                },
                "originStationCode": {
                    "type": "TextUnrestricted",
                    "value": origin_station_name[0]['name']['value']
                },
                "destinationStationCode": {
                    "type": "TextUnrestricted",
                    "value": destination_station_name[0]['name']['value']
                },
                "line": {
                    "type": "TextUnrestricted",
                    "value": parse_value(com['commercialPathInfo']['line'])
                },
                "trafficType": {
                    "type": "Text",
                    "value": parse_value(com['commercialPathInfo']['trafficType'])
                },
                "operator": {
                    "type": "Text",
                    "value": parse_value(com['commercialPathInfo']['opeProComPro']['operator'])
                },
                "product": {
                    "type": "Text",
                    "value": parse_value(com['commercialPathInfo']['opeProComPro']['product'])
                },
                "commercialProduct": {
                    "type": "TextUnrestricted",
                    "value": parse_value(com['commercialPathInfo']['opeProComPro']['commercialProduct'])
                },
                "announceableStations": {
                    "type": "TextUnrestricted",
                    "value": parse_value(com['commercialPathInfo']['announceableStations'][0])
                }
            }
            if com['commercialPathInfo'] is not None and com['commercialPathInfo']['commercialPathKey'] is not None and \
                    com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey'] is not None and \
                    com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey'][
                        'launchingDate'] is not None:
                launchDate = float(
                    com['commercialPathInfo']['commercialPathKey']['commercialCirculationKey']['launchingDate'])
                commercialPath["launchingDate"] = {
                    "type": "DateTime",
                    "value": datetime.fromtimestamp(launchDate / 1000).strftime(datetime_format_cb)
                }
            if com['passthroughStep'] is not None:
                if com['passthroughStep']['stopType'] is not None:
                    commercialPath["stopType"] = {
                        "type": "TextUnrestricted",
                        "value": com['passthroughStep']['stopType']
                    }
                if com['passthroughStep']['stopType'] is not None:
                    commercialPath["stopType"] = {
                        "type": "Text",
                        "value": com['passthroughStep']['stopType']
                    }
                if com['passthroughStep']['arrivalPassthroughStepSides'] is not None and \
                        com['passthroughStep']['arrivalPassthroughStepSides']['plannedTime'] is not None:
                    plannedTime = float(com['passthroughStep']['arrivalPassthroughStepSides']['plannedTime'])
                    commercialPath["plannedTime"] = {
                        "type": "DateTime",
                        "value": datetime.fromtimestamp(plannedTime / 1000).strftime(datetime_format_cb)
                    }
                if com['passthroughStep']['announceable'] is not None:
                    commercialPath["announceable"] = {
                        "type": "Boolean",
                        "value": com['passthroughStep']['announceable']
                    }
                if com['passthroughStep']['departurePassthroughStepSides'] is not None:
                    commercialPath["forecastedOrAuditedDelay"] = {
                        "type": "Number",
                        "value": int(com['passthroughStep']['departurePassthroughStepSides']['forecastedOrAuditedDelay'])
                    }
                    commercialPath["timeType"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['timeType'])
                    }
                    commercialPath["plannedPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['plannedPlatform'])
                    }
                    commercialPath["sitraPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['sitraPlatform'])
                    }
                    commercialPath["ctcPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['ctcPlatform'])
                    }
                    commercialPath["operatorPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['operatorPlatform'])
                    }
                    commercialPath["resultantPlatform"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['resultantPlatform'])
                    }
                    commercialPath["observation"] = {
                        "type": "Text",
                        "value": normalizer(
                            parse_value(com['passthroughStep']['departurePassthroughStepSides']['observation'])
                        )
                    }
                    commercialPath["circulationState"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['circulationState'])
                    }
                    commercialPath["announceState"] = {
                        "type": "Text",
                        "value": parse_value(com['passthroughStep']['departurePassthroughStepSides']['announceState'])
                    }
                entities.append(commercialPath)
        return entities


def Main() -> None:
    # Set the retry configuration
    retriesConfiguration = urllib3.Retry(
        total=post_retry_connect, backoff_factor=1, status_forcelist=[500, 404],
        raise_on_status=False, method_whitelist=["GET", "POST"]
    )

    # Create a session object and set the timeout and retries
    session = requests.Session()
    adapter = TimeoutHTTPAdapter(
        timeout=timeout,
        # The allowed API rate is 200 per hour.
        # We set it to 150 / hour to allow for an initial
        # burst of 50 messages, so if there are less than
        # 50 paths,  we can end quickly.
        api_req_per_sec=150.0/3600.0,
        api_burst=50.0,
        max_retries=retriesConfiguration
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    # Timeout attribute does not work in requests version 2.5.0 or higher
    # session.timeout = timeout
    session.verify = False
    logger.info('*** SEND ENTITIES: Starting ***')
    auth: tc.auth.authManager = tc.auth.authManager(endpoint=endpoint_keystone,
                                                    service=service,
                                                    subservice=subservice_cfg,
                                                    user=user,
                                                    password=password)

    # send entities
    cbm: tc.cb.cbManager = tc.cb.cbManager(endpoint=endpoint_cb,
                                           timeout=timeout,
                                           post_retry_connect=post_retry_connect,
                                           post_retry_backoff_factor=post_retry_backoff_factor,
                                           sleep_send_batch=sleep_send_batch,
                                           batch_size=batch_size)

    stations = get_stations(session)
    logger.info(stations)

    entities_arrivals = get_arrivals(session, stations, cbm, auth)
    logger.info(entities_arrivals)

    entities_departures = get_departures(session, stations, cbm, auth)
    logger.info(entities_departures)

    logger.info('*** SEND ENTITIES STATIONS: Starting ***')
    cbm.send_batch(auth=auth, subservice=subservice_cfg, entities=stations)
    logger.info('*** END SEND ENTITY Done ***')

    logger.info('*** SEND ENTITIES ARRIVALS: Starting ***')
    cbm.send_batch(auth=auth, subservice=subservice_cfg, entities=entities_arrivals)
    logger.info('*** END SEND ENTITY Done ***')

    logger.info('*** SEND ENTITIES DEPARTURES: Starting ***')
    cbm.send_batch(auth=auth, subservice=subservice_cfg, entities=entities_departures)
    logger.info('*** END SEND ENTITY Done ***')


# Main program starts here ###
if __name__ == "__main__":
    try:
        Main()
        logger.info("ETL OK")
    except Exception as err:
        logger.error(f"ETL KO: {err}")
        logger.error(traceback.format_exc())
        sys.exit(-1)
4	Cuadro de mandos
Se han desarrollado los siguientes paneles en el entorno URBO para la representación visual de los indicadores del caso de uso.
4.1	Panel general del caso de uso
 
Ilustración 1. Panel general del caso de uso (salidas)

 
Ilustración 2. Panel general del caso de uso (llegadas)
Este panel funciona como puerta de entrada al caso de uso número 11, centrado en la representación de los datos obtenidos mediante la ETL de las Apis proporcionadas por ADIF.
En él, se pueden apreciar los siguientes widgets:

-	En primer lugar, un mapa interactivo donde se muestran las ubicaciones y los nombres de las estaciones de Valencia.
-	A continuación, un widget separador que delimita la sección relativa a las salidas de las estaciones.
-	Debajo a la izquierda, un gráfico de barras horizontales lista las estaciones y permite que la información de los widgets que le siguen sea la propia de aquella sobre la que se haga clic. 
-	Abajo a la derecha, un widget de tipo pie o tarta, con un desglose por categorías de los tipos de trayectos.
-	Más abajo, una tabla contiene el listado de las salidas de las últimas 24 horas para las estaciones y, mediante un clic en el título de la tabla, se facilita la navegación hacia el panel de datos históricos.
-	Por último, la sección de llegadas, cuya disposición y widgets tienen las mismas funcionalidades que los anteriores, pero son específicos para las llegadas.
4.2	Panel de datos históricos de las estaciones
 
 
Ilustración 3. Panel de datos históricos de las estaciones
En este segundo panel, accesible solo desde el panel general, se presenta información ampliada e histórica de las estaciones.
Nuevamente se han dividido en dos secciones las salidas y las llegadas, a través de dos líneas con texto. 
Para visualizar los datos de la estación que el usuario desee, basta con elegirla en el selector de estaciones situado justo debajo de estos separadores.
Los widgets que en él se incluyen son los siguientes:
-	Datos simples estructurados: con el número de trayectos para cada tipología.
-	Series temporales: que muestran la evolución en el tiempo de los trayectos de las categorías anteriores.
De manera adicional, cabe resaltar que en este panel se permite filtrar la franja temporal que el usuario decida, a través del selector de fecha o calendario.
4.3	Panel de datos brutos de las estaciones
 
Ilustración 4. Panel de datos brutos

Al panel de datos brutos se llega navegando a través del selector de pestañas o tabas situado arriba del todo, en el centro del panel de datos históricos (igualmente ocurre con este en sentido contrario).
Este panel consta solamente un par de tablas (una para las llegadas, y otra para las salidas) similares a las del panel de estado general y que detallan información extendida de los trayectos, pero con un carácter temporal más amplio. De igual modo, se puede elegir la estación cuyos datos se quieran visualizar a través de un selector.
4.4	Panel de mapa de trayectos
 
Ilustración 5. Panel de mapa de trayectos de las estaciones
En este panel, se muestra información relacionada con el origen y el destino de los trayectos de ambas estaciones.
La estructura del mismo es:
-	Arriba del todo, un selector a través del cual se puede elegir la estación para visualizar los datos relativos solo a esta.
-	Debajo, dos mapas de calor, uno para cada sentido del trayecto, en el que se representa de esta forma tan visual el número de trayectos para cada sentido y estación o localidad.
-	Por último, un par de widgets de barras horizontales con la misma información de los mapas, pero que permiten, no obstante, que se haga un filtrado de las estaciones en concreto a través del clic en las entidades.


