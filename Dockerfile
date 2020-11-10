FROM python:3-alpine
LABEL maintainer="damiano lombardo <lombardo.damiano@gmail.com"

VOLUME /config

COPY . /pvstats

RUN apk add --no-cache --virtual .build-deps gcc musl-dev \
    && pip install ./pvstats \
    && apk del .build-deps gcc musl-dev

CMD ["/pvstats/bin/pvstats", "--cfg", "/config/pvstats.conf"]
