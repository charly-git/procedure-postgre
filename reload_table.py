#!/usr/bin/python3

import argparse
import logging
import json
import os
import config
import requests

from salesforce import get_SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkApiError
from tabledesc import TableDesc
from time import sleep
from query_bulk import make_query
from createtable import (postgres_escape_name, postgres_escape_str,
                         postgres_table_name)
from csv_to_postgres import get_pgsql_import
from postgres import get_pg, psycopg2
from query import query

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    #main query bulk
    def main():
        parser = argparse.ArgumentParser(description='Start a query job in salesforce')
        parser.add_argument('table', help='table name')
        args = parser.parse_args()

        logging.basicConfig(filename=config.LOGFILE, format=config.LOGFORMAT.format('query_bulk '+args.table), level=config.LOGLEVEL)

        table_name = args.table
        tabledesc = TableDesc(table_name)
        job = make_query(tabledesc)

        logger.info('Created job %s', job)
        print('Created job {}'.format(job))


        download(job)

    
    ## main create table

        sql = get_pgsql_create(args.table)

        pg = get_pg()
        cursor = pg.cursor()
        for line in sql:
            try:
                cursor.execute(line)
            except (Exception, psycopg2.ProgrammingError) as exc:
                logging.error('Error while executing %s', line)
                raise exc
        pg.commit()

    #main csv to postgre 
        parser.add_argument('--autocommit', action='store_true', help='enable autocommit')

        job_csv_to_postgres(args.job, args.autocommit)

    # query to table
    
        parser = argparse.ArgumentParser(description='Refresh a table from salesforce to postgres')
        parser.add_argument('table', help='the table name to refresh')
        args = parser.parse_args()

        logging.basicConfig(filename=config.LOGFILE, format=config.LOGFORMAT.format('query_poll_table '+args.table), level=config.LOGLEVEL)

        sync_table(args.table)

    main()
