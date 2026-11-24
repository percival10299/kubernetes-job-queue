# MySQL job queue

The API accepts text jobs, and a worker claims one queued job, computes its
SHA-256 hash, and stores the result.

## Setup

Run a local, disposable MySQL instance (the passwords below are for this demo
only):

```sh
docker run -d --name job-queue-demo \
  -e MYSQL_ROOT_PASSWORD=demo-root-password \
  -e MYSQL_DATABASE=job_queue \
  -e MYSQL_USER=worker \
  -e MYSQL_PASSWORD=demo-worker-password \
  -p 127.0.0.1:33067:3306 mysql:8.4
```

Once MySQL is ready, create the table:

```sh
docker exec -i job-queue-demo mysql -uworker -pdemo-worker-password job_queue < schema.sql
export MYSQL_DSN='worker:demo-worker-password@tcp(127.0.0.1:33067)/job_queue'
```

If upgrading a database created with the earlier schema, back it up and run
`migration_add_payload_result.sql` once instead of `schema.sql`:

```sh
docker exec -i job-queue-demo mysql -uworker -pdemo-worker-password job_queue < migration_add_payload_result.sql
```

Existing rows receive an empty payload and retain their status and owner.
MySQL DDL is not transactional, so check the schema before retrying a failed
migration.

## Submit and process a job

Start the API in a terminal:

```sh
export MYSQL_DSN='worker:demo-worker-password@tcp(127.0.0.1:33067)/job_queue'
go run ./cmd/api
```

In another terminal, submit a job, run a worker, and retrieve the result:

```sh
export MYSQL_DSN='worker:demo-worker-password@tcp(127.0.0.1:33067)/job_queue'
curl -sS -H 'Content-Type: application/json' \
  -d '{"text":"hello"}' http://127.0.0.1:8080/jobs
go build -o worker .
./worker --id worker-a
curl -sS http://127.0.0.1:8080/jobs/1
printf 'hello' | shasum -a 256
```

`POST /jobs` returns `202 Accepted` and a JSON `id`. Replace `1` in the GET
request with that ID; on an empty database it will be `1`. The GET response
contains `status` and, once available, `result`. Workers process the first
queued job, so older queued jobs are processed first.

The API listens on `127.0.0.1:8080` by default; use `--addr` to change it. It
has no authentication and is intended for local use. A worker locks and claims
the job in a transaction, commits, then computes and saves the result. If it
fails after committing the claim, the job remains `running`; automatic recovery
is not implemented.

## Integration tests

The tests create and drop uniquely named databases; they do not modify the demo
`job_queue` database. Supply a DSN for a disposable MySQL server with
`CREATE DATABASE` and `DROP DATABASE` privileges. For the container above:

```sh
export MYSQL_TEST_DSN='root:demo-root-password@tcp(127.0.0.1:33067)/'
go test -race -count=1 -v ./...
```

The tests cover submission, processing and retrieval, one job against two
worker processes, ten jobs against twelve worker processes, and rollback.
Without `MYSQL_TEST_DSN`, the integration tests are skipped. The pull request
workflow runs them against a MySQL service.
