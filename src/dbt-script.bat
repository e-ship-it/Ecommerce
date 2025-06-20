@echo off

REM == CHECK PWD ==
echo %cd% 

REM ==== CREATE DBT PROJECT FOLDER ====

SET "DIR_NAME=C:\Users\Ekta Sharma\Ekta\project\Ecommerce\dbt_pipeline\"
if not exist "%DIR_NAME%" (
    mkdir "%DIR_NAME%"
)

cd /d "%DIR_NAME%"
echo %cd% 

REM ==== CREATE AND INITIALIZE THE DBT PROJECT ====
SET "DBT_PROJECT_FILE=C:\Users\Ekta Sharma\Ekta\project\Ecommerce\dbt_pipeline\dbt_pipeline\dbt_project.yml"
if exist "%DBT_PROJECT_FILE%" (
    echo dbt project already initialized. Skipping dbt init.
) else (
    echo Initializing dbt project...
    dbt init dbt_pipeline
)

REM ==== To download the dbt_utils package into the dbt_packages directory and make its macros (like unique_combination_of_columns) available to use.===
cd C:\Users\Ekta Sharma\Ekta\project\Ecommerce\dbt_pipeline\dbt_pipeline\
dbt deps
