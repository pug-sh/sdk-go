package pug

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
)

func TestToErrorReturnsAnErrorUnchanged(t *testing.T) {
	original := &Error{Code: connect.CodeNotFound, Message: "boom"}
	if got := toError(original); got != original {
		t.Errorf("toError = %p, want %p", got, original)
	}
}

func TestToErrorKeepsTheConnectCodeAndCause(t *testing.T) {
	cerr := connect.NewError(connect.CodePermissionDenied, errors.New("denied"))
	err := toError(cerr)
	if err.Code != connect.CodePermissionDenied || err.Message != "denied" || !errors.Is(err.Err, cerr) {
		t.Errorf("toError = %#v", err)
	}
	if got := err.Error(); got != "pug: permission_denied: denied" {
		t.Errorf("Error() = %q", got)
	}
}

func TestToErrorWrapsAGenericError(t *testing.T) {
	generic := errors.New("kaboom")
	err := toError(generic)
	if err.Code != 0 || err.Message != "kaboom" || !errors.Is(err, generic) {
		t.Errorf("toError = %#v", err)
	}
	if got := err.Error(); got != "pug: kaboom" {
		t.Errorf("Error() = %q", got)
	}
}

func TestErrorWithoutAMessage(t *testing.T) {
	if got := (&Error{Code: connect.CodeUnavailable}).Error(); got != "pug: unavailable" {
		t.Errorf("Error() = %q", got)
	}
}

func TestErrorIsTheSentinelForItsCode(t *testing.T) {
	for code, sentinel := range map[connect.Code]error{
		connect.CodeCanceled:           ErrCanceled,
		connect.CodeUnknown:            ErrUnknown,
		connect.CodeInvalidArgument:    ErrInvalidArgument,
		connect.CodeDeadlineExceeded:   ErrDeadlineExceeded,
		connect.CodeNotFound:           ErrNotFound,
		connect.CodeAlreadyExists:      ErrAlreadyExists,
		connect.CodePermissionDenied:   ErrPermissionDenied,
		connect.CodeResourceExhausted:  ErrResourceExhausted,
		connect.CodeFailedPrecondition: ErrFailedPrecondition,
		connect.CodeAborted:            ErrAborted,
		connect.CodeOutOfRange:         ErrOutOfRange,
		connect.CodeUnimplemented:      ErrUnimplemented,
		connect.CodeInternal:           ErrInternal,
		connect.CodeUnavailable:        ErrUnavailable,
		connect.CodeDataLoss:           ErrDataLoss,
		connect.CodeUnauthenticated:    ErrUnauthenticated,
	} {
		if err := toError(connect.NewError(code, errors.New("x"))); !errors.Is(err, sentinel) {
			t.Errorf("errors.Is(%v, %v) = false", err, sentinel)
		}
	}
	err := toError(connect.NewError(connect.CodeNotFound, errors.New("no such profile")))
	for _, target := range []error{ErrUnavailable, &Error{Code: connect.CodeNotFound, Message: "no such profile"}, (*Error)(nil)} {
		if errors.Is(err, target) {
			t.Errorf("errors.Is(%v, %v) = true", err, target)
		}
	}
	if generic := toError(errors.New("kaboom")); errors.Is(generic, ErrUnknown) {
		t.Error("a failure outside the RPC layer matched ErrUnknown")
	}
}

func TestErrorUnwrapsToTheContextError(t *testing.T) {
	err := toError(connect.NewError(connect.CodeCanceled, context.Canceled))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(%v, context.Canceled) = false", err)
	}
}
