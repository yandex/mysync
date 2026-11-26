FROM docker:27-cli

RUN apk add --no-cache git python3 py3-pip && \
    python3 -m venv /venv && \
    /venv/bin/pip install --no-cache-dir \
      'faultstorm @ git+https://github.com/munakoiso/faultstorm.git@b933bb31ee25d426c8b7f5a71012afab073ccdd7' \
      'PyMySQL==1.1.2'

ENV PATH="/venv/bin:$PATH"
WORKDIR /app
COPY main.py mysql_client.py actions.py ./
CMD ["sleep", "infinity"]
