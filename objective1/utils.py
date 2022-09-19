import psycopg2
import json
from datetime import datetime

def connection():
    try:
        fp= open("config.json")
        params  = json.load(fp)
    
        host = params['HOST']
        port = params['PORT']
        dbname = params['DB_NAME']
        username = params['USER']
        password = params['PASSWORD']
        print("Connecting to database")

        #establishing the connection
        conn = psycopg2.connect(database=dbname, user=username, password=password, host=host, port= port)
        conn.autocommit = True
        print("Successfully connected to database")
        return conn

    except:
        print("Unable to connect to database")

def populate(objects):
    #print("Reading info from repo vars")     
    try:        
        repostats = objects.json()
        repo_id = repostats['id']
        user_id = repostats['owner']['id']
        user_name = repostats['owner']['login']
        repo_name = repostats['full_name'] 
        url = repostats['url'].replace(":","")
        created_at = datetime.strptime(repostats['created_at'],"%Y-%m-%dT%H:%M:%SZ")
        updated_at = datetime.strptime(repostats['updated_at'],"%Y-%m-%dT%H:%M:%SZ")
        pushed_at = datetime.strptime(repostats['pushed_at'],"%Y-%m-%dT%H:%M:%SZ")
        has_issues = repostats['has_issues']
        has_downloads= repostats['has_downloads']
        open_issues_count = repostats['open_issues_count']
        size = repostats['size']
        fork = repostats['fork']
        fork_count = repostats['forks_count']
        visibility = repostats['visibility']
        watchers = repostats['watchers']
        language = repostats['language']
        description = repostats['description']    
    except Exception as e:
        print("Unable to get data from json object")
        print(e)
        
    ## create connection
    conn = connection()
    
    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    try:
        #insert_statement1 = '''INSERT INTO GITUSER(USER_ID, USER_NAME) VALUES ({0},{1})'''.format(user_id,user_name) 
        insert_statement2 = '''INSERT INTO REPOS(REPO_ID, USER_ID,REPONAME,URL,CREATED_AT,UPDATED_AT,PUSHED_AT,
        HAS_ISSUES,HAS_DOWNLOADS,OPEN_ISSUE_COUNT,SIZE,FORK,FORK_COUNT,VISIBILITY,WATCHERS,LANGUAGE,DESCRIPTION) VALUES ({0},{1},'{2}','{3}','{4}','{5}','{6}',{7},{8},{9},{10},'{11}',{12},'{13}',{14},'{15}','{16}')'''.format(repo_id,user_id,repo_name,url,created_at.date(),updated_at,pushed_at,has_issues,has_downloads,open_issues_count,size,fork,fork_count,visibility,watchers,language,description)
        # Preparing SQL queries to INSERT a record into the database.
        #cursor.execute(insert_statement1)

        cursor.execute(insert_statement2)

        # Commit your changes in the database
        conn.commit()
        print("Records inserted........")

        # Closing the connection
        conn.close()    
        
    except Exception as e:
        print("Unable to insert record ")
        print(e)
    