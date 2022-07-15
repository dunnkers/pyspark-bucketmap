# https://hub.docker.com/r/apache/spark-py
FROM apache/spark-py:v3.3.0

# Install python deps
USER 0
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Install Git
RUN apt-get install git -y