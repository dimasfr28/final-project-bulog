#!/bin/bash
set -e

airflow db migrate

airflow scheduler &

exec airflow webserver
