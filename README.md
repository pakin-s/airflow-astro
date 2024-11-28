Overview
========

Welcome brothers and sisters, this repository's purpose is to give a easy understanding of how Astro Airflow works for Data Migration Between 2 MySql databases.

Quick Start
===========================
If start form scratch, you should create one network that going to connect all containers together.
Starting form create network you want.
```
docker network create <network_name>
```
Run Exchange and Revamp db composes.

To start Astro containers,
```
make start
```
or if need to.
```
make restart
```

Connect all Astro containers to created network,
```
docker network connect data_migration_network airflow-poc_a4fa9a-webserver-1
docker network connect data_migration_network airflow-poc_a4fa9a-scheduler-1
docker network connect data_migration_network airflow-poc_a4fa9a-triggerer-1
```
also both Exchange and Revamp containers.

You can check if all containers connected to network by.
```
docker network inspect <network_name>
```
AND A HUGE NOTE THAT IF CONTAINERS RUNNING IN THE SAME NETWORK THEY USE INTERNAL PORT TO COMMUNICATE, NOT EXTERNAL ONE.

