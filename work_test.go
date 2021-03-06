package que

import "testing"

func TestLockJob(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}

	if j.tx == nil {
		t.Fatal("want non-nil conn on locked Job")
	}
	if j.db == nil {
		t.Fatal("want non-nil pool on locked Job")
	}

	// check values of returned Job
	if j.ID == 0 {
		t.Errorf("want non-zero ID")
	}
	if want := ""; j.Queue != want {
		t.Errorf("want Queue=%q, got %q", want, j.Queue)
	}
	if want := int16(100); j.Priority != want {
		t.Errorf("want Priority=%d, got %d", want, j.Priority)
	}
	if j.RunAt.IsZero() {
		t.Error("want non-zero RunAt")
	}
	if want := "MyJob"; j.Type != want {
		t.Errorf("want Type=%q, got %q", want, j.Type)
	}
	if want, got := "[]", string(j.Args); got != want {
		t.Errorf("want Args=%s, got %s", want, got)
	}
	if want := int32(0); j.ErrorCount != want {
		t.Errorf("want ErrorCount=%d, got %d", want, j.ErrorCount)
	}
	if j.LastError.Valid {
		t.Errorf("want no LastError, got %v", j.LastError)
	}

	// check for advisory lock
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = j.db.QueryRow(query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("want 1 advisory lock, got %d", count)
	}

	if err = j.Delete(); err != nil {
		t.Fatal(err)
	}
}

func TestLockJobAlreadyLocked(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	j2, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j2 != nil {
		t.Fatalf("wanted no job, got %+v", j2)
	}

	err = j.Delete()
	if err != nil {
		t.Fatal("delete job failed:", err)
	}
}

func TestLockJobNoJob(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		t.Errorf("want no job, got %v", j)
	}
}

func TestLockJobCustomQueue(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob", Queue: "extra_priority"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		t.Errorf("expected no job to be found with empty queue name, got %+v", j)
	}

	j, err = c.LockJob("extra_priority")
	if err != nil {
		t.Fatal(err)
	}

	if j == nil {
		t.Fatal("wanted job, got none")
	}

	if err = j.Delete(); err != nil {
		t.Fatal(err)
	}
}

/*
func TestJobConnRace(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done()

	var wg sync.WaitGroup
	wg.Add(2)

	// call Conn and Done in different goroutines to make sure they are safe from
	// races.
	go func() {
		_ = j.Conn()
		wg.Done()
	}()
	go func() {
		j.Done()
		wg.Done()
	}()
	wg.Wait()
}
/*
// Test the race condition in LockJob
/*
func TestLockJobAdvisoryRace(t *testing.T) {
	c := openTestClientMaxConns(t, 2)
	defer truncateAndClose(c.db)

	// *pgx.ConnPool doesn't support pools of only one connection.  Make sure
	// the other one is busy so we know which backend will be used by LockJob
	// below.
	unusedConn, err := c.db.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer c.db.Release(unusedConn)

	// We use two jobs: the first one is concurrently deleted, and the second
	// one is returned by LockJob after recovering from the race condition.
	for i := 0; i < 2; i++ {
		if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
			t.Fatal(err)
		}
	}

	// helper functions
	newConn := func() *pgx.Conn {
		conn, err := pgx.Connect(testConnConfig)
		if err != nil {
			panic(err)
		}
		return conn
	}
	getBackendID := func(conn *pgx.Conn) int32 {
		var backendID int32
		err := conn.QueryRow(`
			SELECT backendid
			FROM pg_stat_get_backend_idset() psgb(backendid)
			WHERE pg_stat_get_backend_pid(psgb.backendid) = pg_backend_pid()
		`).Scan(&backendID)
		if err != nil {
			panic(err)
		}
		return backendID
	}
	waitUntilBackendIsWaiting := func (backendID int32, name string) {
		conn := newConn()
		i := 0
		for {
			var waiting bool
			err := conn.QueryRow(`SELECT pg_stat_get_backend_waiting($1)`, backendID).Scan(&waiting)
			if err != nil {
				panic(err)
			}

			if waiting {
				break
			} else {
				i++
				if i >= 10000 / 50 {
					panic(fmt.Sprintf("timed out while waiting for %s", name))
				}

				time.Sleep(50 * time.Millisecond)
			}
		}

	}

	// Reproducing the race condition is a bit tricky.  The idea is to form a
	// lock queue on the relation that looks like this:
	//
	//   AccessExclusive <- AccessShare  <- AccessExclusive ( <- AccessShare )
	//
	// where the leftmost AccessShare lock is the one implicitly taken by the
	// sqlLockJob query.  Once we release the leftmost AccessExclusive lock
	// without releasing the rightmost one, the session holding the rightmost
	// AccessExclusiveLock can run the necessary DELETE before the sqlCheckJob
	// query runs (since it'll be blocked behind the rightmost AccessExclusive
	// Lock).
	//
	deletedJobIDChan := make(chan int64, 1)
	lockJobBackendIDChan := make(chan int32)
	secondAccessExclusiveBackendIDChan := make(chan int32)

	go func() {
		conn := newConn()
		defer conn.Close()

		tx, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		_, err = tx.Exec(`LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		if err != nil {
			panic(err)
		}

		// first wait for LockJob to appear behind us
		backendID := <-lockJobBackendIDChan
		waitUntilBackendIsWaiting(backendID, "LockJob")

		// then for the AccessExclusive lock to appear behind that one
		backendID = <-secondAccessExclusiveBackendIDChan
		waitUntilBackendIsWaiting(backendID, "second access exclusive lock")

		err = tx.Rollback()
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		conn := newConn()
		defer conn.Close()

		// synchronization point
		secondAccessExclusiveBackendIDChan <- getBackendID(conn)

		tx, err := conn.Begin()
		if err != nil {
			panic(err)
		}
		_, err = tx.Exec(`LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		if err != nil {
			panic(err)
		}

		// Fake a concurrent transaction grabbing the job
		var jid int64
		err = tx.QueryRow(`
			DELETE FROM que_jobs
			WHERE job_id =
				(SELECT min(job_id)
				 FROM que_jobs)
			RETURNING job_id
		`).Scan(&jid)
		if err != nil {
			panic(err)
		}

		deletedJobIDChan <- jid

		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	}()

	conn, err := c.db.Acquire()
	if err != nil {
		panic(err)
	}
	ourBackendID := getBackendID(conn)
	c.db.Release(conn)

	// synchronization point
	lockJobBackendIDChan <- ourBackendID

	job, err := c.LockJob("")
	if err != nil {
		panic(err)
	}
	defer job.Done()

	deletedJobID := <-deletedJobIDChan

	t.Logf("Got id %d", job.ID)
	t.Logf("Concurrently deleted id %d", deletedJobID)

	if deletedJobID >= job.ID {
		t.Fatalf("deleted job id %d must be smaller than job.ID %d", deletedJobID, job.ID)
	}
}
*/
func TestJobDelete(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	if err = j.Delete(); err != nil {
		t.Fatal(err)
	}

	// make sure job was deleted
	j2, err := findOneJob(c.db)
	if err != nil {
		t.Fatal(err)
	}
	if j2 != nil {
		t.Errorf("job was not deleted: %+v", j2)
	}
}

func TestJobDone(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	err = j.Delete()
	if err != nil {
		t.Fatal("delete failed:", err)
	}

	// make sure conn and pool were cleared
	if j.tx != nil {
		t.Errorf("want nil conn, got %+v", j.tx)
	}
	if j.db != nil {
		t.Errorf("want nil pool, got %+v", j.db)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.db.QueryRow(query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}
}

func TestJobDoneMultiple(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	err = j.Delete()
	if err != nil {
		t.Fatal("delete failed", err)
	}
}

func TestJobDeleteFromTx(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	// delete the job
	if err = j.Delete(); err != nil {
		t.Fatal(err)
	}

	// make sure the job is gone
	j2, err := findOneJob(c.db)
	if err != nil {
		t.Fatal(err)
	}

	if j2 != nil {
		t.Errorf("wanted no job, got %+v", j2)
	}
}

/*
func TestJobDeleteFromTxRollback(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j1, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j1 == nil {
		t.Fatal("wanted job, got none")
	}

	// get the job's database connection
	conn := j1.Conn()
	if conn == nil {
		t.Fatal("wanted conn, got nil")
	}

	// delete the job
	if err = j1.Delete(); err != nil {
		t.Fatal(err)
	}

	if err = conn.Rollback(); err != nil {
		t.Fatal(err)
	}

	// mark as done
	j1.Done()

	// make sure the job still exists and matches j1
	j2, err := findOneJob(c.db)
	if err != nil {
		t.Fatal(err)
	}

	if j1.ID != j2.ID {
		t.Errorf("want job %d, got %d", j1.ID, j2.ID)
	}
}
*/

func TestJobError(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.db)

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	msg := "world\nended"
	if err = j.Error(msg); err != nil {
		t.Fatal(err)
	}

	// make sure job was not deleted
	j2, err := findOneJob(c.db)
	if err != nil {
		t.Fatal(err)
	}
	if j2 == nil {
		t.Fatal("job was not found")
	}

	if !j2.LastError.Valid || j2.LastError.String != msg {
		t.Errorf("want LastError=%q, got %q", msg, j2.LastError.String)
	}
	if j2.ErrorCount != 1 {
		t.Errorf("want ErrorCount=%d, got %d", 1, j2.ErrorCount)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.db.QueryRow(query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}
}
