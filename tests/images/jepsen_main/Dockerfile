FROM mysync-jepsen-test-base:latest
ENV LEIN_ROOT=1
COPY . /var/lib/dist/jepsen_main
RUN apt-get -qq update && apt-get install -y libjna-java \
        gnuplot \
        wget \
        openjdk-21-jre-headless && \
    chmod 600 /root/.ssh/id_rsa && \
    wget https://raw.githubusercontent.com/technomancy/leiningen/2.12.0/bin/lein -O /usr/bin/lein && \
    chmod +x /usr/bin/lein && \
    cp -r /var/lib/dist/jepsen_main/jepsen /root/ && \
    cd /root/jepsen && \
    lein install && lein deps
