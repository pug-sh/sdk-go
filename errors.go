package pug

import (
	"errors"

	"connectrpc.com/connect"
)

// Error is the error returned by the read methods. Ingestion never returns errors.
type Error struct {
	// Code is the Connect status code, or zero when the failure did not come from the RPC
	// layer.
	Code connect.Code
	// Message describes the failure, without the code.
	Message string
	// Err is the underlying error.
	Err error
}

func (e *Error) Error() string {
	msg := "pug: "
	if e.Code != 0 {
		msg += e.Code.String()
		if e.Message == "" {
			return msg
		}
		msg += ": "
	}
	return msg + e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func toError(err error) *Error {
	if perr, ok := err.(*Error); ok { //nolint:errorlint // only an unwrapped *Error passes through as is
		return perr
	}
	if cerr, ok := errors.AsType[*connect.Error](err); ok {
		return &Error{Code: cerr.Code(), Message: cerr.Message(), Err: err}
	}
	return &Error{Message: err.Error(), Err: err}
}
