FROM registry.access.redhat.com/ubi8/ubi-init
LABEL maintainer jholloway@redhat.com
LABEL io.k8s.description A bridge between Kafkavents OpenResults to ReportPortal API
LABEL io.openshift.wants kafka,reportportal
LABEL io.openshift.non-scalable true

RUN yum -y install python3-pip git gcc platform-python-devel
RUN pip3 install --upgrade pip
RUN pip install wheel confluent-kafka reportportal-client pytest
RUN pip install --upgrade --force-reinstall urllib3
#RUN pip install --upgrade git+git://github.com/loadtheaccumulator/grandcentralstation/kafkavents-reportportal.git
RUN pip install -i https://test.pypi.org/simple/ pytest-kafkavents
#RUN pip install -U pytest

# get rid of this next stuff when we install via pip
COPY kafkavents_reportportal.py /usr/local/kafkavents-reportportal/
COPY kafkavents /usr/local/kafkavents-reportportal/kafkavents

CMD python3 /usr/local/kafkavents-reportportal/kafkavents_reportportal.py
