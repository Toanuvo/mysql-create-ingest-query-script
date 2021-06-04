from ast import literal_eval
import pandas as pd
import pymysql
import itertools as itt
import sqlalchemy as sqlalch

# By Kevin Zavadlov

MULTI_VALUE_ATTRIBUTES = ['genres', 'keywords', 'production_companies', 'production_countries', 'spoken_languages']
idnames = {'genres': 'id', 'keywords': 'id', 'production_companies': 'id', 'production_countries': 'iso_3166_1',
           'spoken_languages': 'iso_639_1'}
idtype = {'genres': 'int', 'keywords': 'int', 'production_companies': 'int', 'production_countries': 'char(2)',
          'spoken_languages': 'char(2)'}

# TODO FILL OUT
host = ""
user = ""
password = ""
database = ""

def parsecsvdb(filename):
    global MULTI_VALUE_ATTRIBUTES
    global idnames
    global idtype

    # a function to pass to pd.read_csv to turn the MVAs into a list if python dicts
    def clean(x):
        return literal_eval(x)

    # pd.readcsv() needs a dict to index using column names to get a converter function
    converterdict = dict(zip(MULTI_VALUE_ATTRIBUTES, itt.repeat(clean)))

    moviedb = pd.read_csv(filename, converters=converterdict, encoding="utf-8")

    # parse MVA name and id from moviedb into its own table
    MVAdb = []
    for attrib in MULTI_VALUE_ATTRIBUTES:

        # flatten the data frame column into a list of dicts
        newdf = list(itt.chain.from_iterable(moviedb[attrib].tolist()))

        # turn the list of dicts back into a data frame and remove duplicates
        newdf = pd.DataFrame(newdf).drop_duplicates().reset_index(drop=True)

        # swap columns if necssary
        if newdf.columns[0] == 'name':
            newdf = newdf[['id', 'name']]

        MVAdb.append(newdf)

    # parse MVA id and movie id from movie db
    joindbs = [[] for _ in range(len(MULTI_VALUE_ATTRIBUTES))]
    for _, row in moviedb.iterrows():
        for i, attrib in enumerate(MULTI_VALUE_ATTRIBUTES):
            for vals in row[attrib]:
                joindbs[i].append([vals[idnames[attrib]], row['id']])

    # convert lists to dataframes with columns attrib_id and movie_id
    for i, (db, attrib) in enumerate(zip(joindbs, MULTI_VALUE_ATTRIBUTES)):
        joindbs[i] = pd.DataFrame(db, columns=[attrib + '_id', 'movie_id'])

    # drop MVA columns because they are now seperate tables
    moviedb = moviedb.drop(MULTI_VALUE_ATTRIBUTES, axis=1)
    return moviedb, MVAdb, joindbs


def create_movie_DB():
    global MULTI_VALUE_ATTRIBUTES
    global idnames
    global idtype

    global host
    global user
    global password
    global database
    try:
        with pymysql.connect(
                # TODO REMOVE
                charset="utf8",
                host=host,
                user=user,
                password=password,
                database=database
        ) as connection:
            cursor = connection.cursor()
            # create movie table
            print("creating table movies")
            query = '''
            CREATE TABLE movies(
                budget bigint,
                homepage text,
                id int,
                original_language varchar(50),
                original_title text,
                overview text,
                popularity float(10, 6),
                release_date Date,
                revenue bigint,
                runtime int,
                status varchar(20),
                tagline text,
                title text,
                vote_average FLOAT(10, 2),
                vote_count mediumint,    
                PRIMARY KEY (id)
            )'''
            cursor.execute(query)
            
            for name in MULTI_VALUE_ATTRIBUTES:
                print("creating table", name)
                # create attribute tables
                query = '''CREATE TABLE {0}(
                    {1} {2}, 
                    name varchar(100), 
                    PRIMARY KEY ({1}));
                '''.format(name, idnames[name], idtype[name])
                cursor.execute(query)

                # create join tables
                print("creating table moviehas_" + name)
                query = '''CREATE TABLE moviehas_{0}(
                {1} {3}, 
                movie_id int, 
                PRIMARY KEY ({1}, movie_id),  
                FOREIGN KEY ({1})
                    REFERENCES {0}({2})
                    ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (movie_id)
                    REFERENCES movies(id)
                    ON UPDATE CASCADE ON DELETE CASCADE
                );'''.format(name, name + "_id", idnames[name], idtype[name])
                cursor.execute(query)

            connection.close()
    except pymysql.Error as e:
        print(e)


def insert_data(csvfile):
    global MULTI_VALUE_ATTRIBUTES
    global idnames
    global idtype

    global host
    global user
    global password
    global database
    try:
        with pymysql.connect(
                charset="utf8",
                host=host,
                user=user,
                password=password,
                database=database

        ) as connection:
            enginestr = "mysql+pymysql://" + user + ":" + password + "@" + host + "/hw5?charset=utf8"
            engine = sqlalch.create_engine(enginestr)
            cursor = connection.cursor()

            # parse csv and get relations
            moviedb, MVAdb, joindbs = parsecsvdb(csvfile)

            print("inserting into movies")
            moviedb.to_sql('movies', con=engine, if_exists='append', index=False)

            # for each table and join table and their name insert into the db or join table
            for db, joindb, name in zip(MVAdb, joindbs, MULTI_VALUE_ATTRIBUTES):
                print("inserting into", name)
                db.to_sql(name, con=engine, if_exists='append', index=False)
                print("inserting into moviehas_" + name)
                joindb.to_sql("moviehas_" + name, con=engine, if_exists='append', index=False)

            connection.close()
    except pymysql.Error as e:
        print(e)

def execute_queries():
    global host
    global user
    global password
    global database
    try:
        with pymysql.connect(
                # TODO REMOVE
                charset="utf8",
                host=host,
                user=user,
                password=password,
                database=database

        ) as connection:
            cursor = connection.cursor()

            cursor.execute("select avg(budget) from movies;")
            rows = cursor.fetchall()
            print(rows[0])
            print()

            cursor.execute('''
            WITH
                usmovies AS (select * from moviehas_production_countries where production_countries_id like 'US'),
                companynames AS (SELECT movie_id, name from moviehas_production_companies inner join production_companies on production_companies_id = id)
            SELECT title, name FROM usmovies 
 	            inner JOIN companynames on usmovies.movie_id = companynames.movie_id inner join movies on usmovies.movie_id = id;
            ''')
            rows = cursor.fetchall()
            for i in range(min(100, len(rows))):
                print(rows[i])
            print()

            cursor.execute(''' 
            select title, revenue from movies 
		        order by revenue desc 
                limit 5;
            ''')
            rows = cursor.fetchall()
            for i in range(min(100, len(rows))):
                print(rows[i])
            print()

            cursor.execute('''
            with
                mysorsci as (select * from genres inner join moviehas_genres on genres_id = id where name like 'Mystery' or name like 'Science Fiction'),
                mysandsci as (select movie_id as mysandscimovid from mysorsci GROUP BY movie_id having count(movie_id) = 2),
                allgenres as (select name, movie_id as allgenresmovid from genres inner join moviehas_genres on genres_id = id),
                myssciallgenres as (select * from allgenres inner join mysandsci on mysandscimovid = allgenresmovid),
                resultmovies as (select * from myssciallgenres inner join movies on id = allgenresmovid)
            select title, name from resultmovies;
            ''')
            rows = cursor.fetchall()
            for i in range(min(100, len(rows))):
                print(rows[i])
            print()

            cursor.execute(''' select title, popularity from movies where popularity > (select avg(popularity) from movies);''')
            rows = cursor.fetchall()
            for i in range(min(100, len(rows))):
                print(rows[i])

    except pymysql.Error as e:
        print(e)



#create_movie_DB()
#insert_data("tmdb_5000_movies.csv")
#execute_queries()