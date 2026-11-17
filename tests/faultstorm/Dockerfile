FROM docker:27-cli

RUN apk add --no-cache git python3 py3-pip && \
    python3 -m venv /venv && \
    /venv/bin/pip install --no-cache-dir \
      'faultstorm @ git+https://github.com/munakoiso/faultstorm.git@b76b7b8a368c3af452c9114f2e8a8899924dfa4b' \
      'PyMySQL==1.1.2'

ENV PATH="/venv/bin:$PATH"
WORKDIR /app
COPY main.py mysql_client.py actions.py ./
CMD ["sleep", "infinity"]
