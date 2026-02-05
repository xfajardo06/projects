## Presentación: Configuración de KPIs en la ETL de SEGITTUR

Este documento es mi guion de alto nivel para explicar al equipo **cómo configuramos los indicadores (KPIs y OKRs)** de la ETL del vertical *segitturtourism* y qué tiene que hacer cada uno cuando quiera incorporar un nuevo indicador.

---

## 1. Contexto de la reunión

- **Objetivo**  
  Refrescar, de forma sencilla, **el modo de configurar indicadores para la ETL de SEGITTUR**, de manera que seamos capaces de **añadir nuevos KPIs sin tocar código**, únicamente trabajando sobre entidades en el Context Broker y en la plataforma.

- **Mensaje de convocatoria (resumido)**  
  > Buenos días.  
  > Convocamos esta sesión para refrescar el modo de configurar indicadores para la ETL de SEGITTUR.  
  > La idea es que todo el equipo tenga claro cómo se modelan los cálculos en el Context Broker y en la plataforma, y cómo se relacionan con la ETL de KPIs.

---

## 2. Idea clave que quiero transmitir

En una frase, lo que quiero que se lleven es:

> **La forma de configurar un indicador es modelarlo como entidad auxiliar en el Context Broker, elegir qué tipo de cálculo aplica y con qué vista/entidad de datos se alimenta. A partir de ahí, la ETL se encarga de programar, extraer, calcular y publicar las entidades de indicadores en la plataforma.**

Es decir:

- **No añadimos KPIs cambiando el código**.
- **Añadimos KPIs creando o modificando entidades auxiliares** que describen qué queremos calcular.

---

## 3. Enfoque: configuración por entidades auxiliares

Voy a explicar que trabajamos con un **modelo declarativo basado en entidades**:

- Para cada caso de uso/indicador definimos **una entidad auxiliar en el Context Broker**.
- Esa entidad dice:
  - qué KPI es (`entityID`, `name`, `description`),
  - desde qué vista SQL obtiene los datos (`viewName`),
  - qué tipo de cálculo se aplica (`entityType` + campos específicos),
  - con qué dimensiones se desglosa (`category`, `location`, etc.),
  - y cada cuánto tiempo se debe recalcular (`frequency`, `nextRun`).

Estas entidades auxiliares están modeladas mediante **cinco tipos principales**:

- **`CalcAggregation`**  
  Para agregaciones clásicas sobre una vista SQL: `SUM`, `COUNT`, `COUNT DISTINCT`, etc.

- **`CalcDistinctEntities`**  
  Para contar entidades distintas (número de alojamientos, número de museos, etc.).

- **`CalcNewEntities`**  
  Para detectar **nuevas entidades** en un periodo (nuevos alojamientos registrados, nuevas viviendas turísticas, etc.).

- **`CalcRatio`**  
  Para indicadores que son un **ratio** entre dos columnas (grado de ocupación, porcentajes, etc.).

- **`CalcOKR`**  
  Para indicadores de tipo OKR (objetivos/resultados clave), con su propia lógica y atributos.

La idea fundamental que quiero remarcar:

> **Cada tipo de entidad auxiliar encapsula un patrón de cálculo concreto. Añadiendo una entidad de ese tipo, definimos un nuevo KPI sin tocar la lógica de la ETL.**

---

## 4. Ejemplo práctico apoyado en `/assets/etls/kpis/input`

En la carpeta `assets/etls/kpis/input` tenemos varios ficheros CSV que sirven como **plantillas de ejemplo** de cómo se modela cada tipo de cálculo:

- `CalcAggregation.csv`
- `CalcDistinctEntities.csv`
- `CalcNewEntities.csv`
- `CalcRatio.csv`

Voy a comentar brevemente los campos clave de cada uno, como si fuese en la pizarra.

### 4.1. `CalcAggregation.csv`

Campos importantes:

- `entityID`: identificador del cálculo/KPI (ej. `IT002`, `IT004_CAPACIDAD`…).
- `entityType`: aquí `CalcAggregation`.
- `name`, `description`: nombre y descripción funcional del KPI.
- `category`, `location`: dimensiones de análisis (por categoría, región, provincia…).
- `viewName`: vista SQL en Postgres desde donde se leen los datos (ej. `tourism_view_accommodations`).
- `kpiValue`: columna de la vista que se va a agregar (ej. `numberOfBeds`, `overnightStays`).
- `calculation`: operación a aplicar (`SUM`, `COUNT DISTINCT`, etc.).
- `keepLatest`: si se mantiene solo el último valor o se guarda histórico.
- `frequency`: frecuencia de ejecución (`daily`, `weekly`, `monthly`, etc.).
- `enable`: si el cálculo está activado (`true` / `false`).

Mensaje que quiero decir:

> “Con solo definir estos atributos en una entidad de tipo `CalcAggregation`, la ETL sabe:
> - desde qué vista leer (`viewName`),
> - qué columna usar como valor (`kpiValue`),
> - qué operación aplicar (`calculation`),
> - y cómo desglosar el resultado (`category`, `location`).  
> El KPI queda configurado sin necesidad de cambiar código.”

### 4.2. `CalcDistinctEntities.csv`

Campos clave:

- `entityID`, `entityType` (`CalcDistinctEntities`), `name`, `description`.
- `viewName`: vista donde están las entidades que queremos contar.
- `category`: dimensión de desglose.
- `frequency`, `enable`.

Mensaje:

> “Aquí definimos KPIs que cuentan entidades únicas (alojamientos, museos, etc.). La ETL se encarga de aplicar el conteo distinto sobre la vista definida.”

### 4.3. `CalcNewEntities.csv`

Campos clave:

- `entityID`, `entityType` (`CalcNewEntities`), `name`, `description`.
- `viewName`, `category`, `location` (si aplica).
- `frequency`, `enable`.

Mensaje:

> “Este tipo se centra en **altas nuevas** en el periodo. La entidad de configuración indica la vista y las dimensiones, y la lógica compara periodos para detectar qué entidades son nuevas.”

### 4.4. `CalcRatio.csv`

Campos clave:

- `entityID`, `entityType` (`CalcRatio`), `name`, `description`.
- `viewName`, `category`.
- `ratioColumn`: columna que representa el ratio calculado.
- `totalColumn`: columna de referencia (denominador).
- `frequency`, `enable`.

Mensaje:

> “En los ratios tenemos dos columnas: una de valor (`ratioColumn`) y otra de total (`totalColumn`). Un ejemplo típico es el **grado de ocupación** (pernoctaciones/plazas). De nuevo, esto se modela como entidad y la ETL deduce cómo calcularlo.”

Conclusión de esta sección:

> “El patrón se repite: **toda la configuración se hace declarativamente en entidades auxiliares** —como las de estos CSV— y la ETL interpreta esas entidades para construir consultas y cálculos.”

---

## 5. Flujo de la ETL (explicación de alto nivel)

A continuación, quiero resumir el flujo de la ETL con un lenguaje accesible:

1. **Inicialización**
   - La ETL se conecta al Context Broker.
   - Lee todas las entidades auxiliares (`CalcAggregation`, `CalcDistinctEntities`, `CalcNewEntities`, `CalcRatio`, `CalcOKR`) que estén `enable = true`.
   - De cada una toma el `id`, la `frequency`, la vista de origen (`viewName`), parámetros de cálculo y el `nextRun`.

2. **Scheduler interno**
   - Aunque Jenkins lanza la ETL **todos los días**, dentro del código hay un **scheduler interno**.
   - Compara `nextRun` con la fecha actual y decide qué cálculos tocan ese día.
   - También se pueden **forzar ejecuciones puntuales** pasando:
     - una fecha concreta (`ETL_KPIS_DATE`),
     - y una lista de IDs (`ETL_KPIS_IDS`).

3. **Extract (extracción de datos)**
   - Para cada cálculo que corresponde ejecutar:
     - La ETL construye una consulta a Postgres usando la `viewName` indicada en la entidad auxiliar.
     - El rango temporal depende de la frecuencia:
       - diaria → día anterior completo,
       - semanal → semana anterior,
       - mensual → mes anterior.
   - Así aseguramos que siempre trabajamos sobre **periodos cerrados**, evitando datos parciales.

4. **Transform (cálculo de KPI y mapeo)**
   - Según el `entityType` (`CalcAggregation`, `CalcRatio`, etc.), se aplica una lógica u otra.
   - El resultado se transforma a la entidad destino `TourismIndicator` (modelo de indicador turístico que exponemos en la plataforma).

5. **Load (carga en el Context Broker)**
   - Los indicadores calculados se envían al Context Broker de Thinking City:
     - en modo batch (`batch_size`),
     - con reintentos y backoff en caso de error.
   - Tras cada ejecución exitosa de un cálculo:
     - se actualiza el `nextRun`,
     - y se deja trazabilidad en la propia entidad auxiliar y en las entidades `TourismIndicator` generadas.

6. **Trazabilidad y robustez**
   - La ETL registra logs por cálculo.
   - El fallo de un KPI **no detiene** el procesamiento del resto.
   - Hay mecanismos de reintentos frente a errores de comunicación con la API.

Mensaje que quiero remarcar al contar este flujo:

> “El corazón de la solución es que la **lógica de cálculo es genérica**, y lo que cambia de un KPI a otro son las **entidades auxiliares de configuración**.”

---

## 6. Checklist: cómo configurar un nuevo KPI

Quiero cerrar la explicación con una guía muy práctica para el equipo:

1. **Definir el caso de uso**
   - ¿Qué queremos medir?
   - ¿Sobre qué conjunto de datos (qué vista SQL) se apoya el indicador?

2. **Elegir el tipo de cálculo**
   - ¿Es una agregación? → `CalcAggregation`
   - ¿Es un conteo de entidades distintas? → `CalcDistinctEntities`
   - ¿Es la detección de nuevos registros en un periodo? → `CalcNewEntities`
   - ¿Es un ratio entre dos magnitudes? → `CalcRatio`
   - ¿Es un OKR? → `CalcOKR`

3. **Crear la entidad auxiliar en el Context Broker**
   - Rellenar los atributos clave:
     - `entityID`, `name`, `description`.
     - `viewName` (origen de datos en Postgres).
     - `category`, `location` (dimensiones de desglose, si aplican).
     - Columnas específicas del tipo de cálculo (`kpiValue`, `ratioColumn`, `totalColumn`, etc.).
     - `frequency` (diaria, semanal, mensual…).
     - `nextRun` inicial (si aplica).

4. **Activar el cálculo**
   - Marcar `enable = true` en la entidad auxiliar.

5. **Dejar que la ETL haga su trabajo**
   - En la siguiente ejecución:
     - la ETL leerá la nueva entidad,
     - determinará si le toca ejecutarse según `frequency`/`nextRun`,
     - y generará/actualizará las entidades `TourismIndicator` correspondientes.

Mensaje final de esta sección:

> “Si tenemos clara esta checklist, cualquier miembro del equipo puede **incorporar nuevos indicadores sin necesidad de tocar el código de la ETL**.”

---

## 7. Frases cortas para usar en la presentación

Para terminar, dejo algunas frases que puedo usar casi textuales:

- **De apertura**
  - “Hoy vamos a ver cómo configuramos los KPIs de SEGITTUR en la ETL, de forma que añadir un nuevo indicador sea tan sencillo como crear una entidad en el Context Broker.”

- **Sobre el enfoque**
  - “No programamos KPIs uno a uno; los describimos como entidades auxiliares y dejamos que la ETL los interprete.”
  - “Cada tipo de entidad (`CalcAggregation`, `CalcRatio`, etc.) encapsula un patrón de cálculo; nosotros solo rellenamos los parámetros.”

- **Sobre el flujo**
  - “Jenkins lanza la ETL cada día, pero quien decide qué cálculos se ejecutan es el `nextRun` de cada entidad auxiliar.”
  - “La ETL siempre calcula sobre periodos cerrados (día anterior, semana anterior, mes anterior) para evitar datos parciales.”

- **De cierre**
  - “Lo importante es que, entendiendo bien cómo se modelan estas entidades, podemos evolucionar el catálogo de indicadores sin modificar el código, apoyándonos en la plataforma como fuente de configuración y trazabilidad.”

Con este documento tengo un guion claro para la sesión y un material de referencia que puede consultar cualquier persona del equipo.

