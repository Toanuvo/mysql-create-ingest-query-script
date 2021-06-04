# mysql create/ingest/query script
 A python script that creates a mysql database then ingests and parses a csv into the database.
There is a function defined for each task.

Parsecsvdb: Reads data from the given csv file using pandas, 
	then manipulates it into dataframs that match the database schema (main table, join tables, multi_valued_attribute tables). 
	The function assumes the csv is in an acceptable format for the database.
Create_movie_DB: Creates a database and tables to store movies. Host/username/password/database must be filled out within the .py file.
Insert_data: Inserts data from a csv file into a database that has already been created. the function will call parsevscdb on the given csv file,
	 then append the data onto the tables in the database. Host/username/password/database must be filled out within the .py file same as create_movie_db.
Execute_queries: Executes some example queries on an existing movie database
