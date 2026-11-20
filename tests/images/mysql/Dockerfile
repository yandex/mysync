ARG VERSION=""
FROM mysync-test-base${VERSION}:latest
ARG VERSION=""
ENV VERSION="${VERSION}"
COPY . /var/lib/dist/mysql
COPY ./mysync /usr/bin/mysync
RUN bash /var/lib/dist/mysql/setup.sh
ARG FAULTSTORM=""
RUN if [ -n "$FAULTSTORM" ]; then sed -i 's/autorestart=false/autorestart=true/' /etc/supervisor/conf.d/supervisor_mysql.conf; fi
