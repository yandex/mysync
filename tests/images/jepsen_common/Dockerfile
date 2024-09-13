FROM mysync-test-base:latest
RUN (yes | ssh-keygen -m PEM -t rsa -b 2048 -N '' -f /root/test_ssh_key -C root@mysync_jepsen_1 || true) && \
         eval `ssh-agent -s` && cp /root/test_ssh_key.pub /root/.ssh/authorized_keys && \
                                cp /root/test_ssh_key.pub /root/.ssh/id_rsa.pub && \
                                cp /root/test_ssh_key /root/.ssh/id_rsa && ssh-add -k /root/.ssh/id_rsa
COPY ./ssh_config /etc/ssh/ssh_config
COPY ./sshd_config /etc/ssh/sshd_config
