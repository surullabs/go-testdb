package testdb

type tx struct {
	commitFunc   func() (err error)
	rollbackFunc func() (err error)
}

func (t *tx) Commit() error {
	if t.commitFunc != nil {
		return t.commitFunc()
	}
	return nil
}

func (t *tx) Rollback() error {
	if t.rollbackFunc != nil {
		return t.rollbackFunc()
	}
	return nil
}
