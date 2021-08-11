apt-get update
## install python
apt-get install -y python3.6
apt-get install -y python3-pip
apt-get install -y python-virtualenv

## install java
apt-get install -y openjdk-8-jdk

# environment variables for guest
cat >"/etc/profile.d/hca_env.sh" <<'EOF'
# java
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
# pyspark
export PYSPARK_PYTHON=python3
export SPARK_HOME=~/etl_jobs/venv/lib/python3.6/site-packages/pyspark
export PATH=$PATH:$SPARK_HOME/bin
EOF

