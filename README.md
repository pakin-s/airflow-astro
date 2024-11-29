Overview
========

Welcome brothers and sisters, this repository's purpose is to give an easy understanding of how Astro Airflow works for Data Migration Between 2 MySql databases.

Quick Start
===========================
If start form scratch, you should create one network that going to connect all containers together.
Starting form create network you want.
```
docker network create <network_name>
```
Run Exchange and Revamp db composes.

Create file name "airflow_settings.yaml" on your root, then config this.(you can add connection as many as you want)
PS: AND A HUGE NOTE THAT IF CONTAINERS RUNNING IN THE SAME NETWORK THEY USE INTERNAL PORT TO COMMUNICATE, NOT EXTERNAL ONE.
```
airflow:
  connections:
    - conn_id:          # you call connection in dag using this, to see usage search "conn_id" in dag file
      conn_type:        # just search airflow conn_type or GPT can help you on this ;)
      conn_host:        # if running in same network, container name is USABLE here and I myself think that using container name is better that id
      conn_schema:      # db name (optional)
      conn_login:       # db user
      conn_password:    # db password
      conn_port:        # db port (must be internal port, and do not be confused with host, it's 2 diffrent thing!)

    - conn_id:
      conn_type:
      conn_host:
      conn_schema:
      conn_login:
      conn_password:
      conn_port:

  pools: []             # leave this alone, you don't have to care about it now

  variables: []         # leave this alone, you don't have to care about it now
```

To start Astro containers,
First
```
brew install astro
```

Then
```
make start
```
(you can see other make commands in Mafile)

Connect all Astro containers to created network(docker ps to see exact name of containers),
```
docker network connect <network_name> airflow-poc_<...>-webserver-1
docker network connect <network_name> airflow-poc_<...>-scheduler-1
docker network connect <network_name> airflow-poc_<...>-triggerer-1
```
also both Exchange and Revamp containers.

You can check if all containers connected to network by.
```
docker network inspect <network_name>
```

MUST: PLZ CREATE NEW BRANCH IF NEEDED TO EDIT ANYTHING, DON'T MESS WITH MY MAIN!!!
