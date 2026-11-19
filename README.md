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
