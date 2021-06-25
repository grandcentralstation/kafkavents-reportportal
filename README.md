# kafkavents-reportportal
```
python3 kafkavents_reportportal.py

podman build --rm=true -t kafkavents-reportportal:latest .

podman run -it --secret kafka_secret --env-file=kafkavents/env.bash  \
--rm -v /path/to/kafkavents-reportportal/kafkavents:/usr/local/etc/kafkavents:Z \
localhost/kafkavents-reportportal
```

[![Docker Repository on Quay](https://quay.io/repository/loadtheaccumulator/kafkavents-reportportal/status "Docker Repository on Quay")](https://quay.io/repository/loadtheaccumulator/kafkavents-reportportal)

