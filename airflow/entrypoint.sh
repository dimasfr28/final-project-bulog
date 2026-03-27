#!/bin/bash
set -e

airflow db migrate
exec airflow webserver
