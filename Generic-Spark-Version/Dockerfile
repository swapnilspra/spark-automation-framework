FROM ubuntu:18.04

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-virtualenv \
      openjdk-8-jdk \
      wget \
      && rm -rf /var/lib/apt/lists/* \
      && mkdir -p /etl-jobs/lib && cd /etl-jobs/lib && wget --quiet https://jdbc.postgresql.org/download/postgresql-42.2.8.jar

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m virtualenv --python=/usr/bin/python3 $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /etl-jobs

# copy requirements.txt over first, copying whole etl-jobs directory now will trigger 
# changes that will run pip install again
COPY ./data-mart/etl/etl-jobs/requirements.txt .
RUN pip install -r requirements.txt

COPY ./data-mart/etl/etl-jobs .
COPY ./data-mart/etl/etl-jobs/scripts/environment.docker.ini ./scripts/environment.ini

# mount local volume here when running, also make sure 
# environment.ini configured to use these folders
# - see data-mart/etl/etl-jobs/scripts/environment.docker.ini
VOLUME [ "/data" ]
RUN mkdir -p /data/mart && mkdir -p /data/raw

#pyspark
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV SPARK_HOME=/opt/venv/lib/python3.6/site-packages/pyspark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin

# not sure what this does exactly but it wanted this directory to exist
RUN mkdir /tmp/spark-events

CMD [ "python3" ]
