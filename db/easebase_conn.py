import psycopg2
import boto3
import json
import os




def easebase_conn():
    ssm = boto3.client('ssm')
    #ssm = boto3.client('ssm',  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],  region_name='us-east-2')
    param = ssm.get_parameter(Name='db_postgres_easebase_internal', WithDecryption=True )
    db_request = json.loads(param['Parameter']['Value']) 

    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    conn.autocommit = False
    return conn

_conn = None


def get_conn():
    """easebase_conn(), reused across warm Lambda invocations."""
    global _conn
    if _conn is not None and _conn.closed == 0:
        try:
            with _conn.cursor() as cur:
                cur.execute("SELECT 1")
            _conn.commit()
            return _conn
        except psycopg2.Error:
            try:
                _conn.close()
            except psycopg2.Error:
                pass
            _conn = None

    _conn = easebase_conn()
    return _conn
