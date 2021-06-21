FROM registry.access.redhat.com/ubi8/ubi-init
MAINTAINER loadtheaccumulator@gmail.com

RUN yum -y install python3-pip git gcc platform-python-devel
RUN pip3 install --upgrade pip
RUN pip install wheel confluent-kafka reportportal-client
RUN pip install --upgrade --force-reinstall urllib3
#RUN pip install --upgrade git+git://github.com/loadtheaccumulator/grandcentralstation/kafkavents-reportportal.git
COPY kafkavents_reportportal.py .
COPY kafka_conf.json .

CMD python3 kafkavents_reportportal.py
