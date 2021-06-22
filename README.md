# kafkavents-reportportal
```
python3 kafkavents_reportportal.py

podman build --rm=true -t kafkavents-reportportal:latest .

podman run -it --env-file=kafkavents/env.bash  \
--rm -v /path/to/kafkavents-reportportal/kafkavents:/usr/local/etc/kafkavents:Z \
localhost/kafkavents-reportportal
```
