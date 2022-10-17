# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-python
FROM --platform=linux/amd64 python:3.9-bullseye

## Install utils
RUN apt-get update \
    && apt-get install -y zip \
    nano

## Install aws cli 2
RUN curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip \
    && unzip awscliv2.zip && ./aws/install

## Install Open JDK 8
RUN apt-get install -y software-properties-common \
  && apt-add-repository "deb http://security.debian.org/debian-security stretch/updates main" \
  && apt-get update \
  && apt-get install -y openjdk-8-jdk

## Create environment variables
ENV M2_HOME=/opt/apache-maven-3.6.0
ENV SPARK_HOME=/opt/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3
ENV PATH=“${PATH}:${M2_HOME}/bin”

## Add Maven, Spark and AWS Glue Libs to /opt
RUN curl -SsL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz \
    | tar -C /opt -xzf - 
RUN curl -SsL https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-3.0/spark-3.1.1-amzn-0-bin-3.2.1-amzn-3.tgz \
    | tar -C /opt -xf -
# consider udpating to master branch.
RUN curl -SsL https://github.com/awslabs/aws-glue-libs/archive/refs/tags/v3.0.tar.gz \
    | tar -C /opt -xzf -

# Install Glue dependencies
RUN cd /opt/aws-glue-libs-3.0/bin/ \
    && bash -c '. glue-setup.sh'

## Add Python packages
## requirements.txt - code operation
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
## requirements-dev.txt - testing/formatting
COPY ./requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install -r /tmp/requirements-dev.txt

## Update Python path
ENV GLUE_HOME=/opt/aws-glue-libs-3.0
ENV PYTHONPATH=$GLUE_HOME:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$SPARK_HOME/python

EXPOSE 4040

CMD [“bash”]

##### cd /opt/aws-glue-libs-3.0 ./bin/gluepyspark #####