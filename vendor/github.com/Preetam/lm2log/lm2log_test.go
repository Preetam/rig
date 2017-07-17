package lm2log

import (
	"testing"

	"github.com/Preetam/lm2"
)

func TestPrepare(t *testing.T) {
	col, err := lm2.NewCollection("/tmp/lm2log_Prepare.lm2", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Destroy()

	lg, err := New(col)
	if err != nil {
		t.Fatal(err)
	}

	err = lg.Prepare("a")
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := lg.Prepared()
	if err != nil {
		t.Fatal(err)
	}

	if prepared != 1 {
		t.Errorf("expected prepared to be %d, got %d", 1, prepared)
	}
}

func TestPrepareCommit(t *testing.T) {
	col, err := lm2.NewCollection("/tmp/lm2log_PrepareCommit.lm2", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Destroy()

	lg, err := New(col)
	if err != nil {
		t.Fatal(err)
	}

	err = lg.Prepare("a")
	if err != nil {
		t.Fatal(err)
	}

	err = lg.Commit()
	if err != nil {
		t.Fatal(err)
	}

	committed, err := lg.Committed()
	if err != nil {
		t.Fatal(err)
	}

	if committed != 1 {
		t.Errorf("expected committed to be %d, got %d", 1, committed)
	}

	err = lg.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	committed, err = lg.Committed()
	if err != nil {
		t.Fatal(err)
	}

	if committed != 1 {
		t.Errorf("expected committed to be %d, got %d", 1, committed)
	}
}

func TestPrepareRollback(t *testing.T) {
	col, err := lm2.NewCollection("/tmp/lm2log_PrepareRollback.lm2", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer col.Destroy()

	lg, err := New(col)
	if err != nil {
		t.Fatal(err)
	}

	err = lg.Prepare("a")
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := lg.Prepared()
	if err != nil {
		t.Fatal(err)
	}

	if prepared != 1 {
		t.Errorf("expected prepared to be %d, got %d", 1, prepared)
	}

	err = lg.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	prepared, err = lg.Prepared()
	if err == nil {
		t.Errorf("expected an error getting prepared data, but got %d", prepared)
	}
}
