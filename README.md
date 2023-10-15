# Taller de Creación de un ETL Pipeline con Apache Airflow y MySQL

En este taller, aprenderás cómo construir un ETL (Extract, Transform, Load) pipeline utilizando Apache Airflow. El objetivo es extraer datos de tres fuentes diferentes (una API, un archivo CSV y una base de datos), aplicar transformaciones a estos datos y, finalmente, cargarlos en Google Drive como un archivo CSV. También almacenaremos los datos en una base de datos MySQL.

# Conjunto de Datos CSV para Leer:

Utilizaré un conjunto de datos CSV que contiene información sobre canciones y artistas. Puedes encontrar un conjunto de datos de ejemplo en (https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) y (https://www.kaggle.com/datasets/unanimad/grammy-awards)

# Conjunto de Datos para Cargar en la Base de Datos Inicial:

Mi base de datos inicial será MySQL debido a su amplia disponibilidad y facilidad de uso. Asegúrate de tener una instancia de MySQL configurada y accesible.

# Herramientas Utilizadas

Sistema de Gestión de Base de Datos: MySQL será nuestro sistema de gestión de bases de datos.
Visualizaciones: Utilizaremos Power BI para crear gráficos conectados directamente a la base de datos MySQL.
Orquestación de Tareas: Apache Airflow será la herramienta principal para la orquestación y programación de tareas en el flujo de trabajo.

# Pasos a seguir

1. Clonar el Repositorio

Comienza clonando este repositorio en tu máquina local.

2. Asegurar la Instalación de Python

Asegúrate de tener Python instalado en tu sistema.

3. Configuración de la Base de Datos

Opción 1: Instalar MySQL

Instala MySQL en tu sistema y configura una base de datos.

4. Crear un Entorno Virtual

Crea un entorno virtual para este proyecto ejecutando el siguiente comando en tu terminal:

python -m venv nombre_del_entorno

5. Activar el Entorno Virtual

Luego, activa el entorno virtual. Los comandos pueden variar según tu sistema operativo:
source nombre_del_entorno/bin/activate

6. Configuración de Airflow

Si ya tienes Airflow instalado en la carpeta de tu repositorio, configura el archivo airflow.cfg. En la sección dags_folder, asegúrate de especificar la ruta de los DAGs. Reemplaza dags por etl_dag para que se vea de la siguiente manera:
javascript

# Notas adicionales

- Durante el proceso ETL, asegúrate de supervisar las tareas individuales y verificar que se ejecuten sin errores.
- Si encuentras problemas durante el taller, consulta la documentación de Apache Airflow o busca ayuda en línea.
- Registra cualquier detalle adicional que consideres importante para tu proyecto o para futuras referencias.


