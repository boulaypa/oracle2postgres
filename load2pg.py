#!/usr/bin/env python3
#-*- coding: utf-8 -*-
#@Filename : ${NAME}
#@Date : ${YEAR}-${MONTH}-${DAY}-${HOUR}-${MINUTE}
#@Project: ${PROJECT_NAME}
#@AUTHOR : patrick boulay

import csv
import pandas as pd
import numpy as np
import argparse
from configparser import ConfigParser
import re
from io import StringIO

from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from sqlalchemy.exc import SQLAlchemyError

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db



def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format( table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def file_read(fname):
        content_array = []
        with open(fname) as f:
                #Content_list is the list that contains the read lines.     
                for line in f:
                        content_array.append(line.rstrip("\n\r"))
        return content_array

def main(args):

    params = config()

    if args["target"] is not None:
        target=args["target"].lower()
    else:
        target=args['tabname'].lower()

    col_file='cols/'+args['tabname']+'.cols'
    col_list = file_read( col_file )

    df = pd.read_csv('data/'+args['tabname']+'.csv', header=None, names=col_list)
    print( df )
    #regexp = re.compile(r'vc__[a-z_0-9]*')
    #for c in col_list:
    #    if regexp.search(c):
    #        df.drop(c, axis=1, inplace=True)
    regexp = re.compile(r'sys*')
    for c in col_list:
        if regexp.search(c):
            df.drop(c, axis=1, inplace=True)
    for c in col_list:
        if c == "tag_hist":
            df.drop(c, axis=1, inplace=True)

    df.rename(columns={'vc__date_tag_hist': 'date_hist'}, inplace=True)

    #df["id_host"].apply(lambda x: x.lower())
    #df['id_host'] = df['id_host'].str.lower() 
    #df['id_host'] = df['id_host'].str.replace(r'cti3700', '')

    #df['id_host'] = df['id_host'].map(lambda x: x.rstrip('cti3700'))
    #psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint "host_vg_pkey"
    #DETAIL:  Key (id_host, vg_name, date_hist)=(sr05209, rootvg, 1970-01-01 00:00:01) already exists.
    #CONTEXT:  COPY host_vg, line 81

    #print ( df )
    # iterating the columns 
    for col in df.columns: 
            print(col) 

    try:
        # try something
        url='postgresql://'+params['user']+':'+params['password']+'@'+params['host']+'/'+params['database']
        engine = create_engine(url, connect_args={'options': '-csearch_path={}'.format(params['schema'])})
        if args["truncate"]:
            engine.execute(sa_text('''TRUNCATE TABLE '''+target+''' ''').execution_options(autocommit=True))
        df.to_sql(target, engine, method=psql_insert_copy,if_exists='append', index=False)

    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print( error )

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("--tabname", required=True )
    ap.add_argument("--target", required=False )
    ap.add_argument('--truncate', dest='truncate', action='store_true')
    ap.add_argument('--no-truncate', dest='truncate', action='store_false')
    ap.set_defaults(truncate=True)
    args = vars(ap.parse_args())
    main(args)
