### SnowPy

#### Introduction 

This repo is for providing self serve load capabilities into Snowflake for smaller files (specifically to enable non technical user to create tables and upload data to Snowflake)

#### ::TODO::

This repo is packaged using poetry and is expected to be run using poetry package manager

1. Follow setup instructions on [Poetry site](https://python-poetry.org/docs/) 
2. Clone this repo to your environment 
3. navigate to cloned repo destination on terminal 
4. run poetry shell ( this project requires python <=3.8 to be on your machine and on your path variable) this will create venv
5. exit poetry shell back to main terminal
6. run poetry install which will install all dependencies
7. remove smple file from folders those are there so that the folders get created when you git clone the repo
8. add you files to corresponding file type folder under the upload path (csv, json ,parquet)
9. run main.py or ./bat_files/run_load.bat (windows) or ./bat_files/run_load.sh (linux)
10. fill in prompts for job

PD: The cleanup setting will delete files after upload if set to True





