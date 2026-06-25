package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresSchemaValidation(t *testing.T) {
	tests := []struct {
		name        string
		columns     map[string]schemaColumn
		constraints map[string][]string
		wantErr     bool
	}{
		{
			name:        "compatible table",
			columns:     postgresTestColumns(),
			constraints: map[string][]string{"frames_unique": {"queue_id", "stream", "seq", "direction"}},
		},
		{
			name:        "missing column",
			columns:     withoutColumn(postgresTestColumns(), "updated_at"),
			constraints: map[string][]string{"frames_unique": {"queue_id", "stream", "seq", "direction"}},
			wantErr:     true,
		},
		{
			name:        "nullable column",
			columns:     withNullableColumn(postgresTestColumns(), "queue_id"),
			constraints: map[string][]string{"frames_unique": {"queue_id", "stream", "seq", "direction"}},
			wantErr:     true,
		},
		{
			name:        "missing unique key",
			columns:     postgresTestColumns(),
			constraints: map[string][]string{"other_unique": {"queue_id", "stream"}},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			db := openFakePostgres(t, fakePostgresState{
				columns:     tt.columns,
				constraints: tt.constraints,
			})

			// When
			store, err := NewPostgres(db)

			// Then
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, store)
			} else {
				require.NoError(t, err)
				require.NotNil(t, store)
			}
		})
	}
}

func postgresTestColumns() map[string]schemaColumn {
	columns := make(map[string]schemaColumn)
	for name, notNull := range requiredColumns() {
		columns[name] = schemaColumn{notNull: notNull}
	}
	columns["id"] = schemaColumn{notNull: true}
	return columns
}

func withoutColumn(columns map[string]schemaColumn, name string) map[string]schemaColumn {
	cloned := cloneColumns(columns)
	delete(cloned, name)
	return cloned
}

func withNullableColumn(columns map[string]schemaColumn, name string) map[string]schemaColumn {
	cloned := cloneColumns(columns)
	cloned[name] = schemaColumn{notNull: false}
	return cloned
}

func cloneColumns(columns map[string]schemaColumn) map[string]schemaColumn {
	cloned := make(map[string]schemaColumn, len(columns))
	for name, column := range columns {
		cloned[name] = column
	}
	return cloned
}

type fakePostgresState struct {
	columns     map[string]schemaColumn
	constraints map[string][]string
}

var fakePostgresRegistry = struct {
	sync.Mutex
	registered bool
	states     map[string]fakePostgresState
}{
	states: make(map[string]fakePostgresState),
}

func openFakePostgres(t *testing.T, state fakePostgresState) *sql.DB {
	t.Helper()
	fakePostgresRegistry.Lock()
	if !fakePostgresRegistry.registered {
		sql.Register("reliablemq_fake_postgres", fakePostgresDriver{})
		fakePostgresRegistry.registered = true
	}
	dsn := fmt.Sprintf("test_%s", t.Name())
	fakePostgresRegistry.states[dsn] = state
	fakePostgresRegistry.Unlock()

	db, err := sql.Open("reliablemq_fake_postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		fakePostgresRegistry.Lock()
		delete(fakePostgresRegistry.states, dsn)
		fakePostgresRegistry.Unlock()
	})
	return db
}

type fakePostgresDriver struct{}

func (fakePostgresDriver) Open(name string) (driver.Conn, error) {
	fakePostgresRegistry.Lock()
	state, ok := fakePostgresRegistry.states[name]
	fakePostgresRegistry.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown fake postgres dsn %q", name)
	}
	return fakePostgresConn{state: state}, nil
}

type fakePostgresConn struct {
	state fakePostgresState
}

func (fakePostgresConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are not implemented")
}

func (fakePostgresConn) Close() error { return nil }

func (fakePostgresConn) Begin() (driver.Tx, error) { return fakePostgresTx{}, nil }

func (c fakePostgresConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return fakePostgresResult(1), nil
}

func (c fakePostgresConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "information_schema.columns"):
		return c.columnRows(), nil
	case strings.Contains(query, "pg_index"):
		return c.constraintRows(), nil
	default:
		return nil, fmt.Errorf("unexpected query %q", query)
	}
}

func (c fakePostgresConn) columnRows() driver.Rows {
	values := make([][]driver.Value, 0, len(c.state.columns))
	for name, col := range c.state.columns {
		nullable := "YES"
		if col.notNull {
			nullable = "NO"
		}
		values = append(values, []driver.Value{name, nullable})
	}
	return &fakePostgresRows{columns: []string{"column_name", "is_nullable"}, values: values}
}

func (c fakePostgresConn) constraintRows() driver.Rows {
	var values [][]driver.Value
	for name, cols := range c.state.constraints {
		for _, col := range cols {
			values = append(values, []driver.Value{name, col})
		}
	}
	return &fakePostgresRows{columns: []string{"constraint_name", "column_name"}, values: values}
}

type fakePostgresTx struct{}

func (fakePostgresTx) Commit() error   { return nil }
func (fakePostgresTx) Rollback() error { return nil }

type fakePostgresResult int64

func (r fakePostgresResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakePostgresResult) RowsAffected() (int64, error) { return int64(r), nil }

type fakePostgresRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *fakePostgresRows) Columns() []string { return r.columns }
func (r *fakePostgresRows) Close() error      { return nil }

func (r *fakePostgresRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
