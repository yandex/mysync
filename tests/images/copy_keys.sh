for i in mysql1 mysql2 mysql3; do
  docker exec mysync_${i}_1 rm /root/.ssh/id_rsa
  docker exec mysync_${i}_1 ssh-keygen -m PEM -t rsa -b 2048 -N '' -C root@mysync_${i}_1 -f /root/.ssh/id_rsa
done

for i in mysql1 mysql2 mysql3; do
  docker cp mysync_${i}_1:/root/.ssh/id_rsa.pub id_rsa.pub
  for j in mysql1 mysql2 mysql3; do
    cat id_rsa.pub
    docker exec mysync_${j}_1 /bin/bash -c "echo '$(cat id_rsa.pub)' >>/root/.ssh/authorized_keys"
  done
done

rm id_rsa.pub
