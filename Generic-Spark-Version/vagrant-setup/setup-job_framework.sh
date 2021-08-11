ln -s /hca/data-mart/etl/etl-jobs ~/etl_jobs 
# create virtualenv
cd /hca/data-mart/etl/etl-jobs
virtualenv --python=python3.6 venv
source ./venv/bin/activate
mkdir /tmp/spark-events
# install depedencies
pip install -r requirements.txt

# install flyway
wget -qO- https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/6.3.1/flyway-commandline-6.3.1-linux-x64.tar.gz | tar xvz

sudo ln -s /home/vagrant/flyway-6.3.1/flyway /usr/local/bin/flyway

# create the database tables
flyway migrate -configFiles=/hca/data-mart/databases/datamart/dev.conf -locations=filesystem:/hca/data-mart/databases/datamart

# download postgresql jdbc driver
wget https://jdbc.postgresql.org/download/postgresql-42.2.8.jar -P ./lib/

# copy the environment.local.ini to environment.ini
cp scripts/environment.local.ini scripts/environment.ini

# create the directories
mkdir -p /hca/data
mkdir -p /hca/data/mart
mkdir -p /hca/data/raw

# 
# add example - source venv. run fund_composite. add the file.
# add vagrant and gitignore

