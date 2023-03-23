
set -xe

cat <<EOF > /etc/apt/apt.conf.d/01buildconfig
APT::Install-Recommends "0";
APT::Get::Assume-Yes "true";
APT::Install-Suggests "0";
EOF

apt-get update

apt-get install \
    wget \
    ca-certificates \
    lsb-release \
    gpg-agent \
    apt-utils \
    software-properties-common

apt-key add - < /var/lib/dist/base/percona.gpg
add-apt-repository 'deb http://mirror.yandex.ru/mirrors/percona/percona/apt bionic main'

# common
apt-get update
apt-get install \
    apt-utils \
    openjdk-11-jre-headless \
    less \
    bind9-host \
    net-tools \
    netcat \
    iputils-ping \
    sudo \
    telnet \
    git \
    python3-pip \
    python3-setuptools \
    faketime \
    rsync \
    vim \
    curl \
    iptables
rm -rf /var/run
ln -s /dev/shm /var/run

# ssh
apt-get install  openssh-server
mkdir -p /run/sshd
cp /var/lib/dist/base/sshd_config /etc/ssh/sshd_config
mkdir /root/.ssh
chmod 0700 /root/.ssh
yes | ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
chmod 0600 /root/.ssh/*

# mysql
if [[ "$MYSQL_VERSION" == "8.0" ]]; then
  curl -O https://repo.percona.com/apt/percona-release_latest.generic_all.deb
  sudo apt install gnupg2 lsb-release ./percona-release_latest.generic_all.deb
  percona-release setup ps80
  apt-get update
  apt-get install \
    percona-server-server \
    percona-xtrabackup-80
  rm -rf /var/lib/mysql/auto.cnf
else
  apt-get install \
      percona-server-server-${MYSQL_VERSION} \
      percona-xtrabackup-24
    rm -rf /var/lib/mysql/*
fi

# supervisor
pip3 install git+https://github.com/Supervisor/supervisor.git@4619168a4d820b37641a4719e211cf867bd7f49d
mkdir -p /etc/supervisor/conf.d
cp /var/lib/dist/base/supervisor.conf /etc/supervisor/supervisord.conf
cp /var/lib/dist/base/supervisor_ssh.conf /etc/supervisor/conf.d

# zookeeper
# wget -nc -O - --quiet https://www-eu.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz | tar -xz -C /opt && \
wget -nc -O - --quiet  https://downloads.apache.org/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz | tar -xz -C /opt && \
mv /opt/apache-zookeeper* /opt/zookeeper
