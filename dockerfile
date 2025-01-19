FROM apache/airflow:2.10.1

COPY ./requirements.txt .
# my internet sucks!
RUN pip config set global.timeout 360

RUN python3 -m pip install --upgrade pip \
&& pip install --upgrade setuptools \
&& pip install -r ./requirements.txt

USER airflow