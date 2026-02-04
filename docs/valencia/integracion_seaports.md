 






DESARROLLO DE LA INICIATIVA
CONNECTA VLCi
Expediente:  094/21-SP



Caso de uso 13. Intercambio de datos entre el ayuntamiento de Valencia-Puerto de Valencia
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
2.2	Modelo DB	16
2.2.1	Scripts inicialización DB	16
3	Extracción de información de fuentes externas	18
3.1	Objetivo de la ETL del vertical de puertos	18
3.2	Elementos necesarios	18
3.3	Método de autenticación	19
3.4	Funcionamiento	19
3.5	Ejecución del proceso de ETL	19
3.5.1	Configuración	19
3.5.2	Ejecución de la ETL principal	20
3.6	Código de la ETL	20
4	Cuadro de mandos	31
4.1	Panel general del caso de uso	31
4.2	Panel de datos históricos de las escalas	32
4.3	Panel de datos brutos de las escalas	32

 
Índice de ilustraciones
Ilustración 1. Panel general del caso de uso	31
Ilustración 2. Panel de datos históricos	32
Ilustración 3. Panel de datos brutos	32
 
1	Introducción
1.1	Objetivo
El presente documento describe los elementos desarrollados y desplegados en plataforma para la construcción del caso de uso 13: Intercambio de datos entre el ayuntamiento de Valencia y el puerto de Valencia.
1.2	Destinatarios y Confidencialidad
El destinatario del presente documento es la Dirección Técnica del proyecto designada a tal efecto por Red.es y el Ayto. de València.
El acceso a este documento está restringido a la Dirección Técnica del proyecto. Cualquier acceso fuera de este ámbito, debe ser autorizado de forma expresa por la Dirección Técnica de Red.es y el Ayto. de València. 
1.3	Mantenimiento y Revisiones
Este documento es elaborado y mantenido por Telefónica.
2	Modelo de datos 
Se detalla a continuación el modelo de datos que definirá la información integrada en la plataforma, proveniente a través de procesos de extracción de la información de los datos de transporte marítimo de diferentes fuentes.
2.1	Modelo NGSI
El modelo de información NGSI representa información de contexto como entidades, que tienen propiedades y se relacionan con otras entidades. Cada entidad y relación recibe una referencia única como identificador, lo que hace que los datos correspondientes se puedan exportar como conjuntos de datos de datos enlazados. Para este caso de uso, se parte de modelos estándar FIWARE, adaptados a la necesidad del proyecto.
2.1.1	Entidades principales
PortCall
Este modelo de datos está diseñado para proporcionar información sobre las PortCalls (la visita de un barco a un puerto -o escala-). Permite representar las propiedades de cada PortCall, incluyendo el buque visitante (cargado parcialmente y referenciado a la entidad “Vessel” para más información). En cada atributo se incluyen referencias relacionadas con elementos de otros estándares ampliamente conocidos. El modelo de datos tiene como objetivo proporcionar la información básica sobre una PortCall, es decir, los datos relativos a la llegada y la salida del buque del puerto, pero no las actividades intermedias (atracado, operaciones, ...) que se definen en otras entidades vinculadas (Berth, Operation, ...).
Referencia de campos PortCall:
Atributo	ngsiType	dbType	description	example	extra	unit	range
TimeInstant	DateTime	timestamp with time zone	Time stamp of the data	"2021-12-01T00:46:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
refVesselId	TextUnrestricted	Text	Id of the vessel	"VesselName 001"	Model:'https://schema.org/Text'.
-	-
portCallId	TextUnrestricted	Text	Id of the portCall	"portCallId 001"	Model:'https://schema.org/Text'.
-	-
UNLOCODE	TextUnrestricted	Text	Unique identifier of a port	"ESHUV"	Model:'https://schema.org/Text'.
-	-
stmRegister	TextUnrestricted	Text	Registro STM de la escala	"E36/24"	Model:'https://schema.org/Text'.
-	-
portCallNumber	TextUnrestricted	Text	Primer número de la escala	"1234/2024"	Model:'https://schema.org/Text'.
-	-
portCallNumber2	TextUnrestricted	Text	Segundo número de la escala	"1234/2024"	Model:'https://schema.org/Text'.
-	-
center	TextUnrestricted	Text	Centro desde donde se realiza la escala	"CCS Huelva"	Model:'https://schema.org/Text'.
-	-
forecastReportingDate	DateTime	timestamp with time zone	Fecha de reporte de previsión sobre la que llegará la embarcación	"2021-12-01T00:46:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
forecastETA	DateTime	timestamp with time zone	Tiempo estimado sobre el que llegará la embarcación	"2021-12-01T11:35:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
forecast	TextUnrestricted	Text	Maniobra que se prevee que hará la embarcación	"Atraque"	Model:'https://schema.org/Text'.
-	-
arrivalFirstReportDate	DateTime	timestamp with time zone	Fecha sobre el reporte que se tiene de que una embarcación quiere entrar al área	"2021-12-01T11:35:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
arrivalVTSDate	DateTime	timestamp with time zone	Fecha en la que la embarcación llegó al área	"2021-12-01T11:35:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
arrivalOneHour	TextUnrestricted	Text	Estancia de la embarcación de 1 hora	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalTwoHours	TextUnrestricted	Text	Estancia de la embarcación de 2 horas	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalAuthorized	TextUnrestricted	Text	Autorización sobre la embarcación a realizar la escala	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalLastPort	TextUnrestricted	Text	Último puerto en el que estuvo la embarcación	"BARCELONA (ESBCN)"	Model:'https://schema.org/Text'.
-	-
arrivalArrivalPort	TextUnrestricted	Text	Puerto de entrada de la embarcación	"HUELVA (ESHUV)"	Model:'https://schema.org/Text'.
-	-
arrivalAgent	TextUnrestricted	Text	Agente de la embarcación	"AARUS MARITIMA"	Model:'https://schema.org/Text'.
-	-
arrivalPosition	geo:json	geometry(Point)	Boat position when it arrived at the designated zone	""location": { "type": "geo:json", "value": { "type": "Point", "coordinates": [-6.961715, 37.254327] } }"	Property. Geo:json - Point.	-	-
arrivalPassengersNumber	Number	double precision	Número de pasajeros de la embarcación	21	Model:'https://schema.org/Text'.
-	-
meInstant	DateTime	timestamp with time zone	Time stamp of the data	"2021-12-01T00:46:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
refVesselId	TextUnrestricted	Text	Id of the vessel	"VesselName 001"	Model:'https://schema.org/Text'.
-	-
portCallId	TextUnrestricted	Text	Id of the portCall	"portCallId 001"	Model:'https://schema.org/Text'.
-	-
UNLOCODE	TextUnrestricted	Text	Unique identifier of a port	"ESHUV"	Model:'https://schema.org/Text'.
-	-
stmRegister	TextUnrestricted	Text	Registro STM de la escala	"E36/24"	Model:'https://schema.org/Text'.
-	-
portCallNumber	TextUnrestricted	Text	Primer número de la escala	"1234/2024"	Model:'https://schema.org/Text'.
-	-
portCallNumber2	TextUnrestricted	Text	Segundo número de la escala	"1234/2024"	Model:'https://schema.org/Text'.
-	-
center	TextUnrestricted	Text	Centro desde donde se realiza la escala	"CCS Huelva"	Model:'https://schema.org/Text'.
-	-
forecastReportingDate	DateTime	timestamp with time zone	Fecha de reporte de previsión sobre la que llegará la embarcación	"2021-12-01T00:46:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
forecastETA	DateTime	timestamp with time zone	Tiempo estimado sobre el que llegará la embarcación	"2021-12-01T11:35:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
forecast	TextUnrestricted	Text	Maniobra que se prevee que hará la embarcación	"Atraque"	Model:'https://schema.org/Text'.
-	-
arrivalFirstReportDate	DateTime	timestamp with time zone	Fecha sobre el reporte que se tiene de que una embarcación quiere entrar al área	"2021-12-01T11:35:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
arrivalVTSDate	DateTime	timestamp with time zone	Fecha en la que la embarcación llegó al área	"2021-12-01T11:35:00Z"	Property. Model:'https://schema.org/DateTime'.
-	-
arrivalOneHour	TextUnrestricted	Text	Estancia de la embarcación de 1 hora	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalTwoHours	TextUnrestricted	Text	Estancia de la embarcación de 2 horas	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalAuthorized	TextUnrestricted	Text	Autorización sobre la embarcación a realizar la escala	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalLastPort	TextUnrestricted	Text	Último puerto en el que estuvo la embarcación	"BARCELONA (ESBCN)"	Model:'https://schema.org/Text'.
-	-
arrivalArrivalPort	TextUnrestricted	Text	Puerto de entrada de la embarcación	"HUELVA (ESHUV)"	Model:'https://schema.org/Text'.
-	-
arrivalAgent	TextUnrestricted	Text	Agente de la embarcación	"AARUS MARITIMA"	Model:'https://schema.org/Text'.
-	-
arrivalPosition	geo:json	geometry(Point)	Boat position when it arrived at the designated zone	""location": { "type": "geo:json", "value": { "type": "Point", "coordinates": [-6.961715, 37.254327] } }"	Property. Geo:json - Point.	-	-
arrivalPassengersNumber	Number	double precision	Número de pasajeros de la embarcación	21	Model:'https://schema.org/Text'.
-	-
arrivalCrewNumber	Number	double precision	Número de tripulantes de la embarcación	12	Model:'https://schema.org/Text'.
-	-
arrivalMaxDraft	Number	double precision	Calado máximo de la embarcación	7.5	Model:'https://schema.org/Text'.
-	-
arrivalDeficiencies	TextUnrestricted	Text	Deficiencias de la embarcación	"S\u00ed"	Model:'https://schema.org/Text'.
-	-
arrivalDeficienciesText	TextUnrestricted	Text	Descripción de las deficiencias	"Deficiencias a la entrada"	Model:'https://schema.org/Text'.
-	-
arrivalObservations	TextUnrestricted	Text	Observaciones a la entrada de la embarcación	"Algunas observaciones de entrada"	Model:'https://schema.org/Text'.
-	-
arrivalTotalIMOLoad	Number	double precision	Carga total regulada por la IMO	50.53	Model:'https://schema.org/Text'.
-	-
departureOtherLoadsType	TextUnrestricted	Text	Tipo de otras cargas que lleve la embarcación	"Lastre"	Model:'https://schema.org/Text'.
-	-
departureOtherLoadsAmount	TextUnrestricted	Text	Cantidad de otras cargas que lleve la embarcación	"12"	Model:'https://schema.org/Text'.
-	-
departureOtherLoadsObservations	TextUnrestricted	Text	Observaciones de otras cargas que lleve la embarcación	"Observaciones de la carga"	Model:'https://schema.org/Text'.
-	-

Ejemplo de PortCall (en NGSIv2):
{
    "id": "ENT-SINGLETON-PORTCALL",
    "type": "PortCall",
    "TimeInstant": {
        "type": "DateTime",
        "value": "2021-12-01T00:46:00Z"
    },
    "refVesselId": {
        "type": "TextUnrestricted",
        "value": "VesselName 001"
    },
    "portCallId": {
        "type": "TextUnrestricted",
        "value": "portCallId 001"
    },
    "UNLOCODE": {
        "type": "TextUnrestricted",
        "value": "ESHUV"
    },
    "stmRegister": {
        "type": "TextUnrestricted",
        "value": "E36/24"
    },
    "portCallNumber": {
        "type": "TextUnrestricted",
        "value": "1234/2024"
    },
    "portCallNumber2": {
        "type": "TextUnrestricted",
        "value": "1234/2024"
    },
    "center": {
        "type": "TextUnrestricted",
        "value": "CCS Huelva"
    },
    "forecastReportingDate": {
        "type": "DateTime",
        "value": "2021-12-01T00:46:00Z"
    },
    "forecastETA": {
        "type": "DateTime",
        "value": "2021-12-01T11:35:00Z"
    },
    "forecast": {
        "type": "TextUnrestricted",
        "value": "Atraque"
    },
    "arrivalFirstReportDate": {
        "type": "DateTime",
        "value": "2021-12-01T11:35:00Z"
    },
    "arrivalVTSDate": {
        "type": "DateTime",
        "value": "2021-12-01T11:35:00Z"
    },
    "arrivalOneHour": {
        "type": "TextUnrestricted",
        "value": "Sí"
    },
    "arrivalTwoHours": {
        "type": "TextUnrestricted",
        "value": "Sí"
    },
    "arrivalAuthorized": {
        "type": "TextUnrestricted",
        "value": "Sí"
    },
    "arrivalLastPort": {
        "type": "TextUnrestricted",
        "value": "BARCELONA (ESBCN)"
    },
    "arrivalArrivalPort": {
        "type": "TextUnrestricted",
        "value": "HUELVA (ESHUV)"
    },
    "arrivalAgent": {
        "type": "TextUnrestricted",
        "value": "AARUS MARITIMA"
    },
    "arrivalPosition": {
        "type": "geo:json",
        "value": "`\"location\": { \"type\":   \"geo:json\", \"value\": { \"type\": \"Point\", \"coordinates\": [-6.961715, 37.254327] } }`"
    },
    "arrivalPassengersNumber": {
        "type": "Number",
        "value": 21
    },
    "arrivalCrewNumber": {
        "type": "Number",
        "value": 12
    },
    "arrivalMaxDraft": {
        "type": "Number",
        "value": 7.5
    },
    "arrivalDeficiencies": {
        "type": "TextUnrestricted",
        "value": "Sí"
    },
    "arrivalDeficienciesText": {
        "type": "TextUnrestricted",
        "value": "Deficiencias a la entrada"
    },
    "arrivalObservations": {
        "type": "TextUnrestricted",
        "value": "Algunas observaciones de entrada"
    },
    "arrivalTotalIMOLoad": {
        "type": "Number",
        "value": 50.53
    },
    "arrivalIMOLoads": {
        "type": "StructuredValue",
        "value": "AARUS MARITIMA"
    },
    "arrivalOtherLoadsType": {
        "type": "TextUnrestricted",
        "value": "Lastre"
    },
    "arrivalOtherLoadsAmount": {
        "type": "TextUnrestricted",
        "value": "12"
    },
    "arrivalOtherLoadsObservations": {
        "type": "TextUnrestricted",
        "value": "Observaciones de la carga"
    },
    "departureCommunicationDate": {
        "type": "DateTime",
        "value": "2021-12-01T11:35:00Z"
    },
    "departureVTSDate": {
        "type": "DateTime",
        "value": "2021-12-01T11:35:00Z"
    },
    "departureAuthorized": {
        "type": "TextUnrestricted",
        "value": "Sí"
    },
    "departureDestinationPort": {
        "type": "TextUnrestricted",
        "value": "HUELVA (ESHUV)"
    },
    "departureETA": {
        "type": "DateTime",
        "value": "2021-12-01T11:35:00Z"
    },
    "departurePassengersNumber": {
        "type": "Number",
        "value": 21
    },
    "departureCrewNumber": {
        "type": "Number",
        "value": 12
    },
    "departureMaxDraft": {
        "type": "Number",
        "value": 7.5
    },
    "departureDeficiencies": {
        "type": "TextUnrestricted",
        "value": "Sí"
    },
    "departureDeficienciesText": {
        "type": "TextUnrestricted",
        "value": "Deficiencias a la entrada"
    },
    "departureObservations": {
        "type": "TextUnrestricted",
        "value": "Algunas observaciones de entrada"
    },
    "departureTotalIMOLoad": {
        "type": "Number",
        "value": 50.53
    },
    "departureIMOLoads": {
        "type": "StructuredValue",
        "value": "AARUS MARITIMA"
    },
    "departureOtherLoadsType": {
        "type": "TextUnrestricted",
        "value": "Lastre"
    },
    "departureOtherLoadsAmount": {
        "type": "TextUnrestricted",
        "value": "12"
    },
    "departureOtherLoadsObservations": {
        "type": "TextUnrestricted",
        "value": "Observaciones de la carga"
    }
}
2.2	Modelo DB
Para la persistencia de los datos del caso de uso se han desplegado las siguientes tablas en el datasource PostgreSQL:
Ficheros SQL asociados al modelo PortCallhubs_lastdata.sql: 
•	portcall_lastdata.sql: Fichero SQL del flujo lastdata (tipo FLOW_LASTDATA) en modelo PortCall
2.2.1	Scripts inicialización DB
CREATE TABLE IF NOT EXISTS :target_schema.seaports_portcall_lastdata (
  timeinstant timestamp with time zone,
  refvesselid Text,
  portcallid Text,
  unlocode Text,
  stmregister Text,
  portcallnumber Text,
  portcallnumber2 Text,
  center Text,
  forecastreportingdate timestamp with time zone,
  forecasteta timestamp with time zone,
  forecast Text,
  arrivalfirstreportdate timestamp with time zone,
  arrivalvtsdate timestamp with time zone,
  arrivalonehour Text,
  arrivaltwohours Text,
  arrivalauthorized Text,
  arrivallastport Text,
  arrivalarrivalport Text,
  arrivalagent Text,
  arrivalposition geometry(Point),
  arrivalpassengersnumber double precision,
  arrivalcrewnumber double precision,
  arrivalmaxdraft double precision,
  arrivaldeficiencies Text,
  arrivaldeficienciestext Text,
  arrivalobservations Text,
  arrivaltotalimoload double precision,
  arrivalimoloads Text,
  arrivalotherloadstype Text,
  arrivalotherloadsamount Text,
  arrivalotherloadsobservations Text,
  departurecommunicationdate timestamp with time zone,
  departurevtsdate timestamp with time zone,
  departureauthorized Text,
  departuredestinationport Text,
  departureeta timestamp with time zone,
  departurepassengersnumber double precision,
  departurecrewnumber double precision,
  departuremaxdraft double precision,
  departuredeficiencies Text,
  departuredeficienciestext Text,
  departureobservations Text,
  departuretotalimoload double precision,
  departureimoloads Text,
  departureotherloadstype Text,
  departureotherloadsamount Text,
  departureotherloadsobservations Text,
  -- Common model attributes
  entityid text,
  entitytype text,
  recvtime timestamp with time zone,
  fiwareservicepath text,
  -- PRIMARY KEYS
  CONSTRAINT seaports_portcall_lastdata_pkey PRIMARY KEY (entityid)
);

3	Extracción de información de fuentes externas
La API provee información relacionada con puertos dentro de un rango de fechas específico.
•	URLs:
o	Autenticación: https://www.valenciaportpcs.net/oauth/connect/token 
o	Información de puertos: https://SEAPORTScloud.com/openAPIv0/v1/rest 
•	Parámetros:
o	dateFrom: Fecha inicial (YYYY-MM-DDTHH:MM:SS.SSSZ).
o	dateTo: Fecha final (YYYY-MM-DDTHH:MM:SS.SSSZ).
3.1	Objetivo de la ETL del vertical de puertos
El objetivo de la ETL es procesar y cargar en el Context Broker los datos de puertos. Actualmente, se dispone de un proceso adaptado a los siguientes tipos de análisis:
•	Datos de puertos de Valencia (https://github.com/telefonicasc/sc_vlci-project/issues/211).
3.2	Elementos necesarios
Software
Para poder ejecutar el proceso de ETL en un equipo es necesario disponer del siguiente software:
Software	Versión
Python	>= 3.8
pip	*
Dentro del directorio con el código del proceso (etls) se encuentra un fichero requirements.txt que contiene las dependencias que necesita el script para funcionar adecuadamente. Es recomendable crear un entorno virtual de Python¹ ² (virtualenv), acceder al mismo e instalar dichas dependencias mediante el comando pip install -r requirements.txt (recuerde ubicarse en su shell dentro del directorio etls).
Ejemplo:
  $ cd assets/etls/seaports-integration
  $ virtualenv venv
  $ source venv/bin/activate
  (venv)$ pip install -r requirements.txt
3.3	Método de autenticación
Autenticación
Se utiliza autenticación por token donde se hace una primera solicitud POST al servicio de autenticación enviando los datos de usuario y contraseña para finalmente obtener en la respuesta el token de seguridad.
{
    "username": "user_api",
    "password": "password_api"
}
3.4	Funcionamiento
La ejecución de la ETL está programada para ser ejecutada diariamente de forma automática:
•	Se debe obtener el token mediante credenciales básicas (user_api y password_api).
•	Usar el token en el header de autorización (Bearer <access_token>).
•	Enviar una solicitud GET con los parámetros dateFrom y dateTo.
•	Si no se especifican fechas, se usa el día anterior por defecto
3.5	Ejecución del proceso de ETL
3.5.1	Configuración
La información obtenida de la API de estaciones se almacena en forma de entidades tipo PortCall en el Context Broker del cliente. La aplicación necesita de las siguientes variables de entorno para conectar al context broker y autenticarse:
•	ETL_SEAPORTS_ENVIRONMENT_ENDPOINT_KEYSTONE: URL de conexión al keystone, por ejemplo Url de autenticación con el CB
•	ETL_SEAPORTS_ENVIRONMENT_ENDPOINT_CB: URL de conexión a Orion, por ejemplo Url del CB
•	ETL_SEAPORTS_ENVIRONMENT_SERVICE: Nombre del servicio.
•	ETL_SEAPORTS_ENVIRONMENT_SUBSERVICE: Nombre del subservicio.
•	ETL_SEAPORTS_ENVIRONMENT_USER: Usuario de plataforma con permiso para escribir entidades en el servicio y subservicio especificado.
•	ETL_SEAPORTS_ENVIRONMENT_PASSWORD: Contraseña del usuario especificado.
•	ETL_SEAPORTS_SETTINGS_URL_TOKEN: Url para obtener el token de la API.
•	ETL_SEAPORTS_SETTINGS_URL_BASE: Url base de la api de Puertos.
•	ETL_SEAPORTS_SETTINGS_USER_API:Usuario de la API de Puertos.
•	ETL_SEAPORTS_SETTINGS_PASSWORD_API:Password de la API de Puertos.
•	ETL_SEAPORTS_SETTINGS_DATE_FROM: Fecha en formato yyyy-mm-dd desde para obtener los datos.
•	ETL_SEAPORTS_SETTINGS_DATE_TO: Fecha en formato yyyy-mm-dd hasta para obtener los datos.
•	ETL_SEAPORTS_SETTINGS_BATCH_PAGE_SIZE: Tamaño de batch a ser enviado. Por ejemplo 80.
•	ETL_SEAPORTS_SETTINGS_SLEEP_SEND_BATCH: Tiempo de espera entre envios de batch. Por ejemplo 5 en segundos.
•	ETL_SEAPORTS_SETTINGS_TIMEOUT: Tiempo de espera para obtener una respuesta de los endpoint. Por ejemplo 10 en segundos.
•	ETL_SEAPORTS_SETTINGS_POST_RETRY_CONNECT: Cantidad de reintentos de invocaciones a endpoint. Por ejemplo 3.
•	ETL_SEAPORTS_SETTINGS_POST_RETRY_BACKOFF_FACTOR: Factor de espera entre cada reintento. Por ejemplo 20 en segundos.
•	ETL_SEAPORTS_SETTINGS_IS_DEBUG: Indica si el logger está en modo debug.
•	ETL_LOG_LEVEL: Indica el nivel del logger.
Estas variables pueden estar definidas en el entorno, o pueden almacenarse en un fichero env.json en el mismo directorio que la ETL.
3.5.2	Ejecución de la ETL principal
Pasos para la ejecución de la ETL:
1.	Definir las variables de entorno adecuadas en el archivo config.py tal como se indica en el apartado Variables de entorno.
2.	Asegurarse que se especifiquen correctamente las variables ETL_AENA_API_SETTINGS_CONSUMER_KEY y ETL_AENA_API_SETTINGS_SECRET_KEY ya que determinan la correcta generación del token.
3.	Iniciar el proceso situando su shell dentro del directorio etls/AENA_API. Tras esto, se ha de ejecutar el siguiente comando:
python etl.py
3.6	Código de la ETL
# Copyright 2024 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
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

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from time import time
from typing import Any, Iterable, Optional, Dict
import requests
import tc_etl_lib as tc
import urllib3
import os

from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

load_dotenv()

urllib3.disable_warnings()

# Get the config variables
endpoint_cb = os.getenv('ETL_SEAPORTS_ENVIRONMENT_ENDPOINT_CB')
endpoint_keystone = os.getenv('ETL_SEAPORTS_ENVIRONMENT_ENDPOINT_KEYSTONE')
service = os.getenv('ETL_SEAPORTS_ENVIRONMENT_SERVICE')
subservice_cfg = os.getenv('ETL_SEAPORTS_ENVIRONMENT_SUBSERVICE')
user = os.getenv('ETL_SEAPORTS_ENVIRONMENT_USER')
password = os.getenv('ETL_SEAPORTS_ENVIRONMENT_PASSWORD')
user_api = os.getenv('ETL_SEAPORTS_SETTINGS_USER_API')
password_api = os.getenv('ETL_SEAPORTS_SETTINGS_PASSWORD_API')
url_get_token = os.getenv('ETL_SEAPORTS_SETTINGS_URL_TOKEN')
url_get_data = os.getenv('ETL_SEAPORTS_SETTINGS_URL_BASE')
date_to = os.getenv('ETL_SEAPORTS_SETTINGS_DATE_TO')
date_from = os.getenv('ETL_SEAPORTS_SETTINGS_DATE_FROM')
retries = int(os.getenv('ETL_SEAPORTS_SETTINGS_POST_RETRY_CONNECT', 3))
batch_page_size = int(os.getenv('ETL_SEAPORTS_SETTINGS_BATCH_PAGE_SIZE', '20'))
sleep_send_batch = int(os.getenv('ETL_SEAPORTS_SETTINGS_SLEEP_SEND_BATCH', '3'))
post_retry_backoff_factor = int(os.getenv('ETL_SEAPORTS_SETTINGS_POST_RETRY_BACKOFF_FACTOR', '2'))
timeout = int(os.getenv('ETL_SEAPORTS_SETTINGS_TIMEOUT', '10'))
is_debug = os.getenv('ETL_SEAPORTS_SETTINGS_IS_DEBUG')
logLevelString = os.getenv('ETL_LOG_LEVEL', 'DEBUG')
date_format = "%Y-%m-%d"
datetime_format_cb = "%Y-%m-%dT%H:%M:00.00Z"
datetime_format_with_zone = "%Y-%m-%dT%H:%M:%S%z"

# sets the logging configuration
logging.basicConfig(
    level=logLevelString,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-SEAPORTS-INTEGRATION | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
# get logger to use it
logger = logging.getLogger()


# Define classes
@dataclass(frozen=True)
class CustomException(Exception):
    """Exception raised for errors in the methods.

    Attributes:
        msg  -- explanation of the error
    """
    msg: str


@dataclass(frozen=True)
class NetworkException(Exception):
    """Exception raised for network errors.

    Attributes:
        msg: the failure message
        url -- URL the request was sent to
        status_code -- response status code
        text -- response body
    """
    msg: str
    url: str
    status_code: int
    text: str


def parse_datetime(datetime_str: str, datetime_format: str) -> str:
    try:
        datetime_obj = datetime.strptime(datetime_str, datetime_format)
    except ValueError:
        raise CustomException(msg='Invalid datetime format')
    return datetime_obj.strftime(datetime_format_cb)


def get_t() -> int:
    return int(time())


class Broker:
    """Broker encapsulates all objects needed to talk to the platform"""

    def __init__(self, cbManager: tc.cb.cbManager, authManager: tc.auth.authManager, store: tc.Store):
        """Initialize the broker"""
        self.cbManager = cbManager
        self.authManager = authManager
        self.store = store

    def get(self, entityId: str, entityType: str) -> Optional[Any]:
        """Get an entity by id and type"""
        result = self.cbManager.get_entities(auth=self.authManager, id=entityId, type=entityType)
        if result is None or len(result) <= 0:
            return None
        return result[0]

    def batch(self, entities: Iterable[Any]):
        """send batch of entities to the store"""
        self.store(entities)


def get_data(session: requests.session) -> Any:


    """Get data from a url"""
    session.auth = (user_api, password_api)
    headers = {

        'Accept': 'application/json;charset=UTF-8'

    }
    body = {
        'grant_type': 'client_credentials',
    }
    response_token = session.post(url_get_token, verify=False, auth=HTTPBasicAuth(user_api, password_api),
                                  headers=headers, data=body)
    entities = []
    if response_token.status_code == 200:
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        dateFrom = (yesterday if date_from is None or date_from != '' else date_from) + 'T00:00:00.000Z'
        dateTo = (yesterday if date_to is None or date_to != '' else date_to) + 'T23:59:59.000Z'
        timestampEntity = dateTo
        params = {
            'dateFrom': dateFrom,
            'dateTo': dateTo,
        }
        headersData = {
            'Authorization': 'Bearer ' + response_token.json()['access_token']
        }
        session.auth = None
        response = session.get(url_get_data, verify=False, headers=headersData, params=params)
        if response.status_code == 200:
            for item in response.json()['SearchResults']:
                entity = {}
                arrivalDate = parse_datetime(item['EstimatedTimeArrival'], datetime_format_with_zone)
                departureDate = parse_datetime(item['EstimatedTimeDeparture'], datetime_format_with_zone)
                entity['id'] = 'ENT-SINGLETON-PORTCALL'
                entity['type'] = 'PortCall'
                entity['TimeInstant'] = {
                    'type': 'DateTime',
                    'value': timestampEntity
                }
                entity['portCallId'] = {
                    'type': 'TextUnrestricted',
                    'value': 'PORTCALL-' + str(item['PortCall'])
                }
                entity['vesselAgent'] = {
                    'type': 'TextUnrestricted',
                    'value': str(item['VesselAgent'])
                }
                if item.get('Terminal') is not None:
                    entity['terminal'] = {
                        'type': 'TextUnrestricted',
                        'value': str(item['Terminal'])
                    }
                else:
                    entity['terminal'] = {
                        'type': 'None',
                        'value': None
                    }
                entity['regularLine'] = {
                    'type': 'TextUnrestricted',
                    'value': str(item['RegularLine'])
                }
                entity['voyageCode'] = {
                    'type': 'TextUnrestricted',
                    'value': str(item['Voyage'])
                }
                entity['status'] = {
                    'type': 'TextUnrestricted',
                    'value': str(item['Status'])
                }
                if entity['status'] == 'FIN':
                    entity['arrivalDateScheduled'] = {
                        'type': 'DateTime',
                        'value': parse_datetime(item['ActualTimeArrival'], datetime_format_with_zone)
                    }
                    entity['departureDateScheduled'] = {
                        'type': 'DateTime',
                        'value': parse_datetime(item['ActualTimeDeparture'], datetime_format_with_zone)
                    }

                entity['arrivalDate'] = {
                    'type': 'DateTime',
                    'value': arrivalDate
                }
                entity['departureDate'] = {
                    'type': 'DateTime',
                    'value': departureDate
                }
                entity['UNLOCODE'] = {
                    'type': 'TextUnrestricted',
                    'value': str(item['DestinationPortUnlocode'])
                }
                entities.append(entity)
        return entities
    else:

        raise NetworkException(msg='Failed to get data from URL', url=url_get_token,
                               status_code=response_token.status_code,
                               text=response_token.text)


def add_string(entity: dict, attrib: str, headers: Dict[str, int], line: list, column: str):
    """add a column to the entity as a text attribute"""
    updated = False
    if column in headers:
        index = headers[column]
        if line[index]:
            updated = True
            entity[attrib] = {
                "type": "TextUnrestricted",
                "value": str(line[index]).strip()
            }
    if not updated:
        entity[attrib] = {
            'type': 'None',
            'value': None
        }


class TimeoutHTTPAdapter(HTTPAdapter):
    """HTTP Adapter for global timeouts. See https://github.com/psf/requests/issues/2011"""

    def __init__(self, timeout=None, **kwargs):
        """Add support for timeout variable"""
        super().__init__(**kwargs)
        self.timeout = timeout

    def send(self, *args, **kwargs):
        """Inject timeout into request"""
        kwargs['timeout'] = self.timeout
        return super().send(*args, **kwargs)


def Main():
    # Set the retry configuration
    retriesConfiguration = urllib3.Retry(total=retries, backoff_factor=1, status_forcelist=[500])

    # Create a session object and set the timeout and retries
    session = requests.Session()
    session.mount('https://', TimeoutHTTPAdapter(timeout=timeout, max_retries=retriesConfiguration))
    session.mount('http://', TimeoutHTTPAdapter(timeout=timeout, max_retries=retriesConfiguration))
    session.timeout = 10
    logger.info('*** SEND ENTITY PortCall: Starting ***')

    cbManager = tc.cb.cbManager(
        endpoint=endpoint_cb,
        timeout=timeout,
        post_retry_connect=retries,
        post_retry_backoff_factor=post_retry_backoff_factor,
        sleep_send_batch=sleep_send_batch,
        batch_size=batch_page_size
    )
    authManager = tc.auth.authManager(
        endpoint=endpoint_keystone,
        service=service,
        subservice=subservice_cfg,
        user=user,
        password=password
    )
    with tc.orionStore(cb=cbManager, auth=authManager, service=service, subservice=subservice_cfg) as store:
        broker = Broker(cbManager, authManager, store)
        entities = get_data(session=session)
        logger.info(entities)
        broker.batch(entities)


### Main program starts here ###
Main()
# declare authManager
logger.info('*** END SEND ENTITY Done ***')

4	Cuadro de mandos
Se han desarrollado los siguientes paneles en el entorno URBO para la representación visual de los indicadores del caso de uso.
4.1	Panel general del caso de uso
 
Ilustración 1. Panel general del caso de uso 
Este panel funciona como puerta de entrada al caso de uso número 13, centrado en la representación de los datos obtenidos mediante la ETL de la Api proporcionadas por el puerto de Valencia.
En él, se pueden apreciar los siguientes widgets:

-	En primer lugar, un mapa interactivo donde se muestra la localización georreferenciada del puerto de Valencia 
-	A su derecha, un gráfico de tipo tarta desglosa el tanto por ciento que representa cada tipo de escala de los barcos entre el total
-	Debajo, una tabla con información detallada de las escalas efectuadas en las últimas 24 horas en el puerto de Valencia. Si se hace clic en el título de esta tabla, se puede acceder a los paneles de detalle de las escalas.
4.2	Panel de datos históricos de las escalas
 
Ilustración 2. Panel de datos históricos
Este primer panel de detalle, de tipo histórico, consta de: 
-	4 gráficos de datos simples estructurados, que muestran el número de escalas por cada estado (finalizadas, autorizadas, operando o previstas)
-	Una serie temporal que aporta información de contexto a los demás widgets en cuanto a la evolución de las escalas en el tiempo y para cada estado.
Por último, haciendo clic en la pestaña de datos brutos situada en la parte superior central de este panel, se navega hacia el siguiente.
4.3	Panel de datos brutos de las escalas
 
Ilustración 3. Panel de datos brutos
Este último panel solo cuenta con una tabla y tiene como finalidad la visualización de la información detallada de las escalas efectuadas en la franja temporal que se defina en el selector temporal o calendario.

