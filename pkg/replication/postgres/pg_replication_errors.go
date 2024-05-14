package postgres

import (
	"errors"
	"fmt"

	"github.com/xataio/pgstream/pkg/replication"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var ErrUnsupportedCopyDataMessage = errors.New("unsupported copy data message")

func parseErrNoticeResponse(errMsg *pgproto3.NoticeResponse) error {
	return &replication.Error{
		Severity: errMsg.Severity,
		Msg: fmt.Sprintf("replication notice response: severity: %s, code: %s, message: %s, detail: %s, schemaName: %s, tableName: %s, columnName: %s",
			errMsg.Severity, errMsg.Code, errMsg.Message, errMsg.Detail, errMsg.SchemaName, errMsg.TableName, errMsg.ColumnName),
	}
}

func mapPostgresError(err error) error {
	if pgconn.Timeout(err) {
		return replication.ErrConnTimeout
	}
	return err
}
