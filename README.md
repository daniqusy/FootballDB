1. create SQL database by running football_db_setup_loading.sql (edit correct file addresses for csv load first) and football_db_viewcreation.sql in MySQL workbench or any identical platform. raw data can be found at (https://www.kaggle.com/datasets/davidcariboo/player-scores)
2. clone git project
3. run python etl_full.py in VSC terminal to upsert SQL data to MongoDB through PyMongo
4. run python python app.py in VSC terminal to launch web app
