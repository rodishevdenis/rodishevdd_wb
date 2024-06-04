```bash
docker run -d \
    -p 5432:5432 \
    -e POSTGRES_USER=rodishev \
    -e POSTGRES_PASSWORD=12345 \
    -e PGDATA=/data/postgres \
    -m 128m \
    --cpus 1 \
    --restart always \
    --network bridge \
    -v ./postgres:/data/postgres \
    --name postgres \
    mirror.gcr.io/postgres:16.3
```

```bash
docker restart postgres
```

```bash
docker rm -f postgres
```

```bash
docker run -d \
    -p 5050:80 \
    -e PGADMIN_DEFAULT_EMAIL=pgadmin4@pgadmin.org \
    -e PGADMIN_DEFAULT_PASSWORD=admin \
    -e PGADMIN_CONFIG_SERVER_MODE='False' \
    -m 128m \
    --cpus 1 \
    --restart always \
    --network bridge \
    -v ./pgadmin:/var/lib/pgadmin \
    --name pgadmin \
    mirror.gcr.io/dpage/pgadmin4:8.7
```

```bash
docker restart pgadmin
```

```bash
docker rm -f pgadmin
```