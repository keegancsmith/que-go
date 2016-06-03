package que

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

func openTestClientMaxConns(t testing.TB, maxConnections int) *Client {
	db, err := sql.Open("postgres", "host=localhost dbname=que-go-test")
	if err != nil {
		t.Fatal(err)
	}
	return NewClient(db)
}

func openTestClient(t testing.TB) *Client {
	return openTestClientMaxConns(t, 5)
}

func truncateAndClose(db *sql.DB) {
	if _, err := db.Exec("TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	db.Close()
}

func findOneJob(q queryable) (*Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &Job{}
	err := q.QueryRow(findSQL).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return j, nil
}
