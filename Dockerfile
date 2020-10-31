FROM python:3-alpine
LABEL maintainer="damiano lombardo <lombardo.damiano@gmail.com"

VOLUME /config

COPY . /pvstats

RUN pip install ./pvstats

CMD ["/pvstats/bin/pvstats", "--cfg", "/config/pvstats.conf"]
