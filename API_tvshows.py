# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 14:42:32 2022

@author: c_ver
"""

from flask import Flask, request
from flask_restx import Api, Resource, reqparse, abort
import json
import requests
import re
import pandas as pd
import sqlite3
from datetime import datetime
from sqlite3 import Error
import time
from flask import send_file
from matplotlib import pyplot as plt
import numpy as np
#import webbrowser

app = Flask(__name__)
api = Api(app, title='TV Shows API',description='created by Cristian Vergara', default="API  Available Methods")


tvshow_args = reqparse.RequestParser()
tvshow_args.add_argument('name', type = str, help = "Insert the name of the tv show you want to import from the TV Maze API", required=True)
@api.route("/tv-shows/import")
class TvShow(Resource):
    @api.response(201, "Created")
    @api.response(404, "Name not found")
    @api.response(400, "Error: Name already in database")
    @api.expect(tvshow_args)
    @api.doc(description="Import a TV Show by its name to the local database")
    #saving tv show from tvmaze into local Database       
    def post(self):
        """Imports tv shows to the local database from TV Maze API"""
        args = tvshow_args.parse_args()
        name = args.get('name')
        #Getting the data from the tvmaze API
        query = "http://api.tvmaze.com/search/shows?q={}".format(name)
        req = requests.get(query)
        #to json
        data = req.json()
        result = 'Null'
        for tvshow in data:
            format_tvshow = re.sub(r'[^A-Za-z0-9 ]+', '', tvshow['show']['name']).lower()
            if name.lower() == format_tvshow:
                #returns the first tv show that matches the query in the case of multiple matches
                result = tvshow['show']
                #deleting unnecessary data
                if 'webChannel' in result.keys():
                    del result['webChannel']
                if 'dvdCountry' in result.keys():            
                    del result['dvdCountry']
                if 'externals' in result.keys():            
                    del result['externals']
                if 'updated' in result.keys():            
                    del result['updated']
                
                df = pd.json_normalize(result)
                
                df["last-update"] = time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())
                df = df.rename(columns = {'id' : 'tvmaze_id'})
                #  check if tvshow already in database:
                try:
                   if str(df['tvmaze_id'][0]) in list(sql_to_df(db_file, table).tvmaze_id):
                       
                       return {"message": "TV Show: {} already in database.".format(name)}, 400
                    
                   else: #dataframe into database
                       
                       df_table = sql_to_df(db_file, table)
                       df['id'] = int(df_table['id'].max()) + 1
                       rearanged_index = ['id','last-update','tvmaze_id', 'url', 'name', 'type', 'language', 'genres', 'status', 'runtime',
                       'premiered', 'officialSite', 'weight', 'summary', 'schedule.time',
                       'schedule.days', 'rating.average', 'network.id', 'network.name',
                       'network.country.name', 'network.country.code',
                       'network.country.timezone', '_links.self.href',
                       '_links.previousepisode.href', '_links.nextepisode.href']
                       df = df.reindex(columns=rearanged_index)
                       df = df.applymap(str)
                       #dropping unnecessary columns
                       df = df.drop(['_links.self.href','_links.previousepisode.href','_links.nextepisode.href' ], axis=1)
                       #writing into sql table of the database
                       df_to_sql(df, db_file, table)
                       
                       result = df.to_json(orient="index")
                       parsed = json.loads(result)
                       
                    
                       query_to_df = sql_to_df(db_file, table)
                       return {"id": parsed['0']['id'],
                                "last-update":parsed['0']['last-update'],
                                "tvmaze-id": parsed['0']['tvmaze_id'],
                                "href": "127.0.0.1:5000/tv-shows/{}".format(parsed['0']['id'])                                
                                }, 201
                    
               
                except pd.io.sql.DatabaseError:  # except: for first value in the database
                    
                    df["id"] = 1
                    rearanged_index = ['id','last-update','tvmaze_id', 'url', 'name', 'type', 'language', 'genres', 'status', 'runtime',
                       'premiered', 'officialSite', 'weight', 'summary', 'schedule.time',
                       'schedule.days', 'rating.average', 'network.id', 'network.name',
                       'network.country.name', 'network.country.code',
                       'network.country.timezone', '_links.self.href',
                       '_links.previousepisode.href', '_links.nextepisode.href']
                    df = df.reindex(columns=rearanged_index)
                    df = df.applymap(str)
                    #dropping unnecessary columns
                    df = df.drop(['_links.self.href','_links.previousepisode.href','_links.nextepisode.href' ], axis=1)
                    #dataframe into database
                    df_to_sql(df, db_file, table)
                    
                    result = df.to_json(orient="index")
                    parsed = json.loads(result)
                    
                    #reading id from sql table
                    query_to_df = sql_to_df(db_file, table)                   
                    return {"id": parsed['0']['id'],
                            "last-update":parsed['0']['last-update'],
                            "tvmaze-id": parsed['0']['tvmaze_id'],
                            "href": "127.0.0.1:5000/tv-shows/{}".format(parsed['0']['id'])
                            
                            }, 201
                   
        if result == 'Null':           
            return {"Error": "TV Show: {} not found".format(name)}, 404


@api.route("/tv-shows/<int:id>")
class tvshow_id(Resource):
    
    @api.response(200, "OK")
    @api.response(404, "TV Show ID not found")
    @api.response(500, "Server Error")
    @api.doc(description="Get the TV Show info by its ID")
    # Retrieving tv show info from local database
    def get(self, id):
        """Returns the info of a tv show from the local database"""
        #  check if tvshow already in database:
        try:             
            if str(id) not in list(sql_to_df(db_file, table).id):              
                api.abort(404, "tv show id: {} does not exists".format(id))
                
            df = sql_to_df(db_file,table)
        except pd.io.sql.DatabaseError: 
            return {"Error": "Database Error"},500
            
        df1 = df.loc[df['id'] == str(id)]
        id_list = list(df['id'])
        result = df1.to_json(orient="index")
        parsed = json.loads(result)
        
        if str(id) == min(id_list): #first id
            if len(id_list) == 1:
                next_tv = 'null' 
                prev_tv = 'null'
                
                _links = { 
                                "self":{
                                "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(id))
                                }}
            else:                
                next_tv = id_list[id_list.index(str(id))+1]
                _links = {                 
                            "self":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(id))
                            },
                            "next":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(next_tv))
                            }
                        }
                
                
        elif str(id) == max(id_list): #last id
            next_tv = 'null'
            prev_tv = id_list[id_list.index(str(id))-1]
            
            _links = {                    
                            "self":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(id))
                            },
                            "previous":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(prev_tv))
                            }
                        }
            
        else:
            next_tv = id_list[id_list.index(str(id))+1]
            prev_tv = id_list[id_list.index(str(id))-1]
            
            _links = {                 
                            "self":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(id))
                            },
                            "previous":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(prev_tv))
                            },
                            "next":{
                            "href": "http://127.0.0.1:5000/tv-shows/{}".format(str(next_tv))
                            }
                        }
        
        parsed['links'] = _links     
        return parsed, 200
        
    #Deleting a tv show from local database
    @api.response(200, "OK")
    @api.response(404, "TV Show ID not found")
    @api.response(500, "Server Error")
    @api.doc(description="Delete a TV Show by its ID")
    def delete(self, id):
        """Deletes a tv show from the local database"""
        try:            
            #  check if tvshow already in database
            if str(id) not in list(sql_to_df(db_file, table).id):
                api.abort(404, "tv show id: {} does not exists in the database".format(id))
            #delete from database    
            delete_sql_by_id(db_file, table, id)        
            return {"message" : "The tv show with id {} was removed from the database!".format(id),
                    "id": id
                    }, 200
        except pd.io.sql.DatabaseError: 
            return {"Error": "Database Error"},500    
    
#retrieving a list of the tv shows in the local database
tv_shows_list_args = reqparse.RequestParser()
tv_shows_list_args.add_argument('order_by', type=str, default="+id",help = "Insert the variable for ordering as shown in default. + for ascending and - for descending order. Maximum 2 variables separated by comma.\n \
                                Variables accepted: {id,name,runtime,premiered,rating-average}")
tv_shows_list_args.add_argument('page', type=int ,default=1)
tv_shows_list_args.add_argument('page_size', type=int ,default=100,help="Number of TV Shows per page")
tv_shows_list_args.add_argument('filter', type=str ,default="id,name", help="Variables for filtering. Must be separated by comma without leaving spaces \n \
                                Variables accepted: {tvmaze_id ,id ,last-update ,name ,type ,language ,genres ,status ,runtime ,premiered ,officialSite ,schedule ,rating-average ,weight ,network.name ,summary}")
@api.route('/tv-shows/')
class tv_shows_list(Resource):
    @api.expect(tv_shows_list_args)
    @api.response(200, 'OK')
    @api.response(404, "Wrong Variable for ordering")
    @api.response(500, "Server Error")
    @api.doc(description="Generate a list of the available TV Shows stored in the local database")
    
    def get(self):
        """Returns a list of the tv shows in the local database"""
        args = tv_shows_list_args.parse_args()
        order_by = args.get('order_by')
        page = args.get('page')
        page_size = args.get('page_size')
        filter = args.get('filter')
        try:            
            df = sql_to_df(db_file,table)
        except pd.io.sql.DatabaseError: 
            return {"Error": "Empty database"},500
            
        #formatting
        if 'rating-average' in order_by:
            order_by = order_by.replace('rating-average', 'rating.average')
        if 'rating-average' in filter:
            filter = filter.replace('rating-average', 'rating.average')
        
        order_by_aux = order_by #for displaying the links
                   
        if "," in order_by:
            order_by = order_by.split(',')
        else:
            order_by = [order_by]
    
        if len(order_by) == 1:    
            variable = re.sub(r'[^A-Za-z.]+', '', order_by[0])
            if order_by[0][0] == "+":
                try:
                    df_2 = df.sort_values(by=variable, ascending=True)
                except KeyError:
                    return "wrong variable:  does not exists in the database.Check the accepted variables",404 
                    
        
            if order_by[0][0] == "-": 
                try:
                    df_2 = df.sort_values(by=variable, ascending=False)
                except KeyError:
                    return "wrong variable:  does not exists in the database.Check the accepted variables",404 
        
        if len(order_by) == 2:
            variable1 = re.sub(r'[^A-Za-z.]+', '', order_by[0])
            variable2 = re.sub(r'[^A-Za-z.]+', '', order_by[1])
            if order_by[0][0] == "+":    
                if order_by[1][0] == "+":
                    try:
                        df_2 = df.sort_values(by=[variable1,variable2], ascending=[True,True])
                    except KeyError:
                        return "wrong variable:  does not exists in the database.Check the accepted variables",404
                if order_by[1][0] == "-":
                    try:                        
                        df_2 = df.sort_values(by=[variable1,variable2], ascending=[True,False])
                    except KeyError:
                        return "wrong variable:  does not exists in the database.Check the accepted variables",404
            
            if order_by[0][0] == "-":    
                if order_by[1][0] == "+":
                    try:                        
                        df_2 = df.sort_values(by=[variable1,variable2], ascending=[False,True])
                    except KeyError:
                        return "wrong variable:  does not exists in the database.Check the accepted variables",404 
                if order_by[1][0] == "-":
                    try:                        
                        df_2 = df.sort_values(by=[variable1,variable2], ascending=[False,False])
                    except KeyError:
                        return "wrong variable:  does not exists in the database.Check the accepted variables",404
        
        df_3 = df_2.filter(items=filter.split(','))
    
        df_3 = df_3.iloc[:int(page_size), :]
        
        if len(df_3) < int(page_size):
            page_size = len(df_3)
        result = df_3.to_json(orient="records")
        parsed = json.loads(result)
        return  {
                "page": page,
                "page-size": page_size,
                "tv-shows": parsed,
                "links" : {"self":{
                            "href": 'http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}'.format(order_by_aux.replace('+','%2B'),page,page_size,filter.replace(',','%2C'))
                            },
                            "next":{
                            "href": 'http:/127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}'.format(order_by_aux.replace('+','%2B'),page+1,page_size,filter.replace(',','%2C'))
                            },
                            },
                }
    
#stats of the database
tv_shows_stats_args = reqparse.RequestParser()
tv_shows_stats_args.add_argument('format', type=str, default="json", required=True, help = "Insert the parameter. \n Accepted parameter {json or image}")
tv_shows_stats_args.add_argument('by', type=str ,default="language",required=True, help = "Insert the attribute you want to consult. \n Accepted attributes: {language, genres, status, type}")
@api.route('/tv-shows/statistics')
class tv_shows_stats(Resource):
    @api.expect(tv_shows_stats_args)
    @api.response(200, 'OK')
    @api.response(400, "Wrong Variable as input. Check the accepted variables")
    @api.response(500, "Server Error")
    @api.doc(description="Generate the statistics for the TV Shows")
    
    def get(self):
        """Returns statistics of the tv shows in the local database"""
        args = tv_shows_stats_args.parse_args()
        format = args.get('format')
        if format not in ['json', 'image']:
            return {"Error": "not soported format"},400
        
        by = args.get('by')
        if by not in ['language', 'genres', 'status', 'type']:
            return {"Error": "not soported attribute"},400
                
        #read sql
        try:            
            df = sql_to_df(db_file,table)
        except pd.io.sql.DatabaseError: 
            return {"Error": "Database Error"},500
       
        #total tv shows
        total_tvshows = df['id'].count()
        #format used for time
        f ="%m/%d/%Y, %H:%M:%S"
        now = datetime.now()
        current_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        #check difference
        df['check time'] = df.apply(lambda tv_show: check_time_difference(datetime.strptime(current_time, f), datetime.strptime(tv_show['last-update'], f)), axis=1)
        #check if days < 1 using an aux column
        df['updated_in_24hrs'] = df.apply(lambda tv_show: tv_show['check time'].days, axis=1)
        updated_df = df["updated_in_24hrs"] < 1
        #updated in <24hrs tv shows
        total_updated = updated_df.count()
        if by == 'language':
            group_by_language = df.groupby('language')['id'].count()
            language_keys =list(group_by_language.keys())
            language_values = list(group_by_language)
            list_language = list(zip(language_keys,language_values))
            df_language = pd.DataFrame(list_language)
            df_language['stats'] = df_language.apply(lambda language: round(language[1]/sum(language_values),2), axis=1)
            df_language_dict = dict(zip(df_language[0], df_language['stats']))
            values = df_language_dict
                    
        if by == 'status':
            group_by_status = df.groupby('status')['id'].count()
            status_keys =list(group_by_status.keys())
            status_values = list(group_by_status)
            list_status = list(zip(status_keys,status_values))
            df_status = pd.DataFrame(list_status)
            df_status['stats'] = df_status.apply(lambda status: round(status[1]/sum(status_values),2), axis=1)
            df_status_dict = dict(zip(df_status[0], df_status['stats']))
            values = df_status_dict
            
        if by == 'type':
            group_by_type = df.groupby('type')['id'].count()
            type_keys =list(group_by_type.keys())
            type_values = list(group_by_type)
            list_type = list(zip(type_keys,type_values))
            df_type = pd.DataFrame(list_type)
            df_type['stats'] = df_type.apply(lambda type_: round(type_[1]/sum(type_values),2), axis=1)
            df_type_dict = dict(zip(df_type[0], df_type['stats']))
            values = df_type_dict
                    
        if by == 'genres':
            group_by_genres = df.groupby('genres')['id'].count()
            genres_keys =group_by_genres.keys()
            list_genre = list(genres_keys)
            elements_aux =[]
            for element in list_genre:
                aux = element.strip('][').split(', ')
                elements_aux.extend(aux)
            
            unique_elements = np.unique(elements_aux)
            dict_aux = {}
            for element in unique_elements:
                count = elements_aux.count(element)
                dict_aux[element] = count
            values = dict_aux
        
        if format == 'json':

            return {"total":int(total_tvshows), 
                    "total-updated": int(total_updated),
                    "values":values},200
        
        if format == 'image':
            if by=='genres':
                #plot bar plot
                keys = list(dict_aux.keys())
                values = list(dict_aux.values())
                fig = plt.figure(figsize = (10, 5))
                plt.title("Quantities of Genre")
                plt.bar(keys, values, color ='green')
                fig.tight_layout()
                img = "image.png"
                plt.savefig(img)
            else:
                #plot pie chart
                keys = list(values.keys())
                values = list(values.values())
                fig = plt.figure(figsize =(5, 5))
                plt.title("Percentaje of {}".format(by))
                plt.pie(values, labels = keys)
                img = "image.png"
                plt.savefig(img)
            
            return send_file(img, mimetype='image/png',cache_timeout=0)
            
                 
##DATABASE FUNCTIONS
def create_connection(db_file):
    """ create a database connection to a SQLite database 
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
            
def create_table(db_file, create_table_sql):
    """
    create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    conn = sqlite3.connect(db_file)
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
    except Error as e:
        print(e)
    conn.close()


def df_to_sql(df, db_file, table_name):
    """
    Write records stored in a Df to a SQL database. 
    """
    conn = sqlite3.connect(db_file)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close() 

def sql_to_df(db_file, table_name):
    """
    read data from sql table. returns df
    """
    conn = sqlite3.connect(db_file)
    query = 'SELECT * FROM {}'.format(table_name)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def delete_sql_by_id(db_file, table_name, id):
    """
    delete row in a sql table by id
    """
    conn = sqlite3.connect(db_file)
    sql = 'DELETE FROM {} WHERE id=?'.format(table_name)
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()
    conn.close()
    

def check_time_difference(t1: datetime, t2: datetime):
    #ref: https://stackoverflow.com/questions/1345827/how-do-i-find-the-time-difference-between-two-datetime-objects-in-python
    t1_date = datetime(
        t1.year,
        t1.month,
        t1.day,
        t1.hour,
        t1.minute,
        t1.second)

    t2_date = datetime(
        t2.year,
        t2.month,
        t2.day,
        t2.hour,
        t2.minute,
        t2.second)

    t_elapsed = t1_date - t2_date

    return t_elapsed
                

##PROGRAM####
    
if __name__ == "__main__":
    
    db_file = 'tv_shows_api.db'
    table = "tv_shows"
    
    #HOST_NAME = 'localhost'
    #PORT = 5000
    
    #sql database
    create_connection(db_file)
    #create table
    sql_create_tv_shows = """
                        CREATE TABLE IF NOT EXISTS tv_shows(
                            id              integer PRIMARY KEY,
                            last-update     text    NOT NULL,
                            tvmaze_id       integer NOT NULL,                          
                            url             text    NOT NULL,
                            name            text    NOT NULL,
                            type            text,    
                            language        text,    
                            genres          text,    
                            status          text,
                            runtime         integer,
                            premiered       text,
                            officialSite    text,
                            weight          int,
                            summary         text,
                            rating_average  real,
                            network_id      int,
                            network_name    text,
                            network_country_name    text,
                            network_country_cod     text,
                            network_country_timezone    text,
                        );
                        """
    create_table(db_file, sql_create_tv_shows)
    app.run(debug=True)
    