package pug

import (
	"errors"

	"connectrpc.com/connect"
)

// Error is the error returned by the read methods and handed to [OnError] when a send fails.
// Ingestion never returns errors. Under [errors.Is] it matches the sentinel for its Code, such
// as [ErrNotFound].
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

// Is reports whether target is the sentinel for e's Code, such as [ErrNotFound]. An Error
// without a Code matches none.
func (e *Error) Is(target error) bool {
	c, ok := target.(codeError)
	return ok && connect.Code(c) == e.Code
}

// The sentinels are values rather than *Error, so they can't be modified, and go vet catches a
// slip like errors.As(err, &ErrNotFound).
type codeError connect.Code

func (c codeError) Error() string { return "pug: " + connect.Code(c).String() }

// Sentinels for the Connect status codes, so errors.Is(err, ErrNotFound) checks an [*Error]'s
// Code.
var (
	ErrCanceled           error = codeError(connect.CodeCanceled)
	ErrUnknown            error = codeError(connect.CodeUnknown)
	ErrInvalidArgument    error = codeError(connect.CodeInvalidArgument)
	ErrDeadlineExceeded   error = codeError(connect.CodeDeadlineExceeded)
	ErrNotFound           error = codeError(connect.CodeNotFound)
	ErrAlreadyExists      error = codeError(connect.CodeAlreadyExists)
	ErrPermissionDenied   error = codeError(connect.CodePermissionDenied)
	ErrResourceExhausted  error = codeError(connect.CodeResourceExhausted)
	ErrFailedPrecondition error = codeError(connect.CodeFailedPrecondition)
	ErrAborted            error = codeError(connect.CodeAborted)
	ErrOutOfRange         error = codeError(connect.CodeOutOfRange)
	ErrUnimplemented      error = codeError(connect.CodeUnimplemented)
	ErrInternal           error = codeError(connect.CodeInternal)
	ErrUnavailable        error = codeError(connect.CodeUnavailable)
	ErrDataLoss           error = codeError(connect.CodeDataLoss)
	ErrUnauthenticated    error = codeError(connect.CodeUnauthenticated)
)

func toError(err error) *Error {
	if perr, ok := err.(*Error); ok { //nolint:errorlint // only an unwrapped *Error passes through as is
		return perr
	}
	if cerr, ok := errors.AsType[*connect.Error](err); ok {
		return &Error{Code: cerr.Code(), Message: cerr.Message(), Err: err}
	}
	return &Error{Message: err.Error(), Err: err}
}
