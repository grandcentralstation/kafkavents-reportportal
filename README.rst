Kafkavents ReportPortal Bridge
==============================

Using the Container
-------------------

Update kafka.json file with latest info::

    podman secret create kafka_secret kafkavents/kafka.json

Build the container (temporary)::

    podman build --rm=true -t kafkavents-reportportal:latest .

Run the container::
    podman run -it --secret kafka_secret --env-file=kafkavents/env.bash  \
--rm -v /path/to/kafkavents-reportportal/kafkavents:/usr/local/etc/kafkavents:Z \
localhost/kafkavents-reportportal


[![Docker Repository on Quay](https://quay.io/repository/loadtheaccumulator/kafkavents-reportportal/status "Docker Repository on Quay")](https://quay.io/repository/loadtheaccumulator/kafkavents-reportportal)
