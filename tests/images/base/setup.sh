set -xe

cat <<EOF >/etc/apt/apt.conf.d/01buildconfig
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

apt-key add - </var/lib/dist/base/percona.gpg
add-apt-repository 'deb http://mirror.yandex.ru/mirrors/percona/percona/apt jammy main'
add-apt-repository 'deb http://mirror.yandex.ru/mirrors/percona/ps-80/apt jammy main'

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
  iptables
rm -rf /var/run
ln -s /dev/shm /var/run

# ssh
apt-get install openssh-server
mkdir -p /run/sshd
cp /var/lib/dist/base/sshd_config /etc/ssh/sshd_config
mkdir /root/.ssh
chmod 0700 /root/.ssh
yes | ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa
cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
chmod 0600 /root/.ssh/*

# mysql
if [[ "$MYSQL_VERSION" == "8.0" ]]; then
  apt-get install \
    percona-server-server=8.0.\* \
    percona-xtrabackup-80
else
  apt-get install \
    percona-xtradb-cluster-server-${MYSQL_VERSION} \
    percona-xtradb-cluster-client-${MYSQL_VERSION} \
    percona-xtradb-cluster-common-${MYSQL_VERSION} \
    percona-xtrabackup-24
fi
rm -rf /var/lib/mysql/*

# supervisor
pip3 install git+https://github.com/Supervisor/supervisor.git@18b59a1403778766561ab49b18cf2558e8a4d227
mkdir -p /etc/supervisor/conf.d
cp /var/lib/dist/base/supervisor.conf /etc/supervisor/supervisord.conf
cp /var/lib/dist/base/supervisor_ssh.conf /etc/supervisor/conf.d

# zookeeper
wget -nc -O - --quiet https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz | tar -xz -C /opt &&
  mv /opt/apache-zookeeper* /opt/zookeeper
