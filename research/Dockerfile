FROM openjdk:8
LABEL maintainer="Haaroon Yousaf (h.yousaf [at] ucl.ac.uk)"

################## BEGIN INSTALLATION ######################
RUN apt-get update && apt-get install -y software-properties-common \
    wget \
    apt-transport-https \
    gnupg \
    curl \
    build-essential \
    checkinstall \
    libreadline-gplv2-dev \
    libncursesw5-dev \
    libssl-dev \
    libsqlite3-dev \
    tk-dev \
    libgdbm-dev \
    libc6-dev \
    libbz2-dev \
    pandoc \
    scala \
    git

# download and install apache spark from the mirror
RUN mkdir -p /opt/spark && \
    wget -q --show-progress http://mirror.ox.ac.uk/sites/rsync.apache.org/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz && \
    tar -xzf spark-2.2.0-bin-hadoop2.7.tgz && \
    cp -r spark-2.2.0-bin-hadoop2.7/* /opt/spark && \
    rm -rf spark-2.2.0-bin-hadoop2.7.tgz spark-2.2.0-bin-hadoop2.7 && \
    cd  /opt/spark && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.2.jar

ENV SPARK_CLASSPATH=/opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

#RUN set -ex && \
#    echo 'deb http://deb.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/jessie-backports.list && \
#    apt-get update -y && \
#    apt-get install -t jessie-backports \
#        --force-yes openjdk-8-jre-headless \
#        ca-certificates-java \
#        openjdk-8-jdk \

RUN apt-get update && apt-get install -y python2.7 python-pip python-dev python-tk
RUN pip install python-bitcoinrpc==1.0 \
        SQLAlchemy==1.1.13 \
        Flask \
        pypandoc \
        jsonschema \
        networkx \
        numpy \
        matplotlib \
        pandas \
        pycrypto \
        python-dateutil \
        requests \
        simplejson \
        ijson && \
    pip install pyspark==2.2.0.post0

RUN echo "spark.driver.extraClassPath = /opt/spark/postgresql-42.2.2.jar" >> /opt/spark/conf/spark-defaults.conf
RUN echo "spark.executor.extraClassPath = /opt/spark/postgresql-42.2.2.jar" >> /opt/spark/conf/spark-defaults.conf

RUN apt-get update && apt-get install -y ipython
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/ipython
ENV SCRIPTS /root/scripts
RUN mkdir -p $SCRIPTS
ADD docker/*.py $SCRIPTS/
ADD docker/runAll.sh $SCRIPTS/
RUN chmod u+x $SCRIPTS/runAll.sh
ENV RESEARCH /root/research
RUN mkdir -p $RESEARCH
ADD docker/addresses/*.* $RESEARCH/

VOLUME ["/root/research"]

CMD ["bash"]
