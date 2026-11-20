# MySQL job claim

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

Once MySQL is ready, create the table and queued job:

```sh
docker exec -i job-queue-demo mysql -uworker -pdemo-worker-password job_queue < schema.sql
export MYSQL_DSN='worker:demo-worker-password@tcp(127.0.0.1:33067)/job_queue'
go build -o worker .
./worker --id worker-a &
./worker --id worker-b &
wait
docker exec job-queue-demo mysql -uworker -pdemo-worker-password job_queue -e 'SELECT * FROM jobs;'
```

Exactly one worker claims job 1. To repeat the race:

```sh
docker exec job-queue-demo mysql -uworker -pdemo-worker-password job_queue \
  -e "UPDATE jobs SET status = 'queued', claimed_by = NULL WHERE id = 1;"
./worker --id worker-a &
./worker --id worker-b &
wait
docker exec job-queue-demo mysql -uworker -pdemo-worker-password job_queue -e 'SELECT * FROM jobs;'
```

The worker locks the first queued row inside a transaction, updates it using
the same transaction, and reports a claim only after the commit succeeds.
Database errors are reported on stderr with a nonzero exit status.

## Integration tests

The tests create and drop uniquely named databases; they do not modify the demo
`job_queue` database. Supply a DSN for a disposable MySQL server with
`CREATE DATABASE` and `DROP DATABASE` privileges. For the container above:

```sh
export MYSQL_TEST_DSN='root:demo-root-password@tcp(127.0.0.1:33067)/'
go test -race -count=1 -v ./...
```

The tests run one job against two worker processes, ten jobs against twelve
worker processes, and a rollback followed by a successful claim. Without
`MYSQL_TEST_DSN`, the integration tests are skipped. The pull request workflow
runs them against a MySQL service.
