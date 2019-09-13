#!/usr/bin/env python3

# Created to add and remove worker on a kubernetes cluster
# Currently we are only handling only one worker node and one master
# TODO: Listen to events on Kubernetes and add worker nodes dynamically
from os import environ
import psycopg2
import signal
import time
from sys import exit, stderr


# adds a host to the cluster
def add_worker(conn, host, port):
    cur = conn.cursor()
    worker_dict = ({'host': host, 'port': port})

    print("adding %s" % host, file=stderr)
    cur.execute("""SELECT master_add_node(%(host)s, %(port)s)""", worker_dict)


# Connect to master through CITUS_MASTER_HOST environment variable
def get_db_connection(db_label, db_host, db_port, db_username, db_password, db_name):
    while True:
        try:
            conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s port=%s" %
                                    (db_name, db_username, db_host, db_password, db_port))
            break
        except psycopg2.OperationalError:
            print("Could not connect to postgresql on db %s. Sleeping for 1 second" % db_label)
            time.sleep(1)

    conn.autocommit = True

    print("connected to %s" % db_label, file=stderr)

    return conn


def setup_cluster():
    # Database credentials
    postgres_pass = environ.get('POSTGRES_PASSWORD', '')
    postgres_user = environ.get('POSTGRES_USER', 'postgres')
    postgres_db = environ.get('POSTGRES_DB', postgres_user)

    # Get connection of master
    citus_master_host = environ.get('CITUS_MASTER_SERVICE_HOST', 'localhost')
    citus_master_port = environ.get('CITUS_MASTER_SERVICE_PORT', 5434)
    conn_master = get_db_connection('citus master', citus_master_host, citus_master_port, postgres_user, postgres_pass,
                                    postgres_db)

    # This is used by Health Check to test for readiness
    open('/manager-ready', 'a').close()

    # Check whether worker is up and running
    citus_worker_host = environ.get('CITUS_WORKER_SERVICE_HOST', 'localhost')
    citus_worker_port = environ.get('CITUS_WORKER_SERVICE_PORT', 5432)
    get_db_connection('citus worker', citus_worker_host, citus_worker_port, postgres_user, postgres_pass,
                      postgres_db)

    # Add the worker to master
    add_worker(conn_master, citus_worker_host, citus_worker_port)


# implemented to make Docker exit faster (it sends sigterm)
def graceful_shutdown(signal, frame):
    print('shutting down...', file=stderr)
    exit(0)


def main():
    print("Starting manager")
    signal.signal(signal.SIGTERM, graceful_shutdown)
    setup_cluster()


if __name__ == '__main__':
    main()
