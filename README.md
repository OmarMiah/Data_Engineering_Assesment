# Fetch Rewards # - ETL off a SQS Qeueue

#### Background Information and Questions
This was a project that simulates and ETL that reads messages from an AWS SQS Queue, transforms that data, and uploads to a Postgres Database. 

I had to make a number of decisions from the following while developling this solution: 

1. How will the messages be read from the queue?
2. What type of data structures should be used?
3. How can I mask the PII data so that duplicate values can be identified?
4. What is a good way to connect to the postgres database? 
5. Where and how will the application run?


# Getting Started

## Project Setup
1. Fork this [repository]( https://github.com/OmarMiah/Data_Engineering_Take_Home_Fetch) to a personal Github, GitLab, Bitbucket, etc... account.
2. You will need the following installed on your local machine
    * make
        * Ubuntu -- `apt-get -y install make`
        * Windows -- `choco install make`
        * Mac -- `brew install make`
    * python3 -- [python install guide](https://www.python.org/downloads/)
    * pip3 -- `python -m ensurepip --upgrade` or run `make pip-install` in the project root
    * awslocal -- `pip install awscli-local`  or run `make pip install` in the project root
    * docker -- [docker install guide](https://docs.docker.com/get-docker/)
    * docker-compose -- [docker-compose install guide]()
3. Run `make start` or `Docker Compose` to execute the docker-compose file in the the project (see scripts/ and data/ directories for more details)
    * An AWS SQS Queue is created
    * A script is run to write 100 JSON records to the queue
    * A Postgres database will be stood up
    * A user_logins table will be created in the public schema
4. Test local access
    * Read a message from the queue using awslocal, `awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue`
    * Connect to the Postgres database, verify the table is created
    * username = `postgres`
    * database = `postgres`
    * password = `postgres`

```bash
# password: postgres

psql -d postgres -U postgres  -p 5432 -h localhost -W
Password: 

postgres=# select * from user_logins;
 user_id | device_type | hashed_ip | hashed_device_id | locale | app_version | create_date 
---------+-------------+-----------+------------------+--------+-------------+-------------
(0 rows)
```
5. Now to run the application, simply open up a terminal in the root of your workspace or project folder where you cloned this project and type: 

```bash
    python3 retrieve_transform_load.py 
```

6. Run `make stop` to terminate the docker containers and optionally run `make clean` to clean up docker resources.
