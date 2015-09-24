// Package stackerr provides a way to augment errors with one or more stack
// traces to allow for easier debugging.
package stackerr

import (
	"errors"
	"fmt"

	"github.com/facebookgo/stack"
)

// Error provides the wrapper that adds multiple Stacks to an error. Each Stack
// represents a location in code thru which this error was wrapped.
type Error struct {
	multiStack *stack.Multi
	underlying error
}

// Error provides a multi line error string that includes the stack trace.
func (e *Error) Error() string {
	return fmt.Sprintf("%s\n%s", e.underlying, e.multiStack)
}

// MultiStack identifies the locations this error was wrapped at.
func (e *Error) MultiStack() *stack.Multi {
	return e.multiStack
}

// Underlying returns the error that is being wrapped.
func (e *Error) Underlying() error {
	return e.underlying
}

type hasMultiStack interface {
	MultiStack() *stack.Multi
}

// WrapSkip the error and add the current Stack. The argument skip is the
// number of stack frames to ascend, with 0 identifying the caller of Wrap. If
// the error to be wrapped has a MultiStack, the current stack will be added to
// it.  If the error to be wrapped is nil, a nil error is returned.
func WrapSkip(err error, skip int) error {
	// nil errors are returned back as nil.
	if err == nil {
		return nil
	}

	// we're adding another Stack to an already wrapped error.
	if se, ok := err.(hasMultiStack); ok {
		se.MultiStack().AddCallers(skip + 1)
		return err
	}

	// we're create a freshly wrapped error.
	return &Error{
		multiStack: stack.CallersMulti(skip + 1),
		underlying: err,
	}
}

// Wrap provides a convenience function that calls WrapSkip with skip=0. That
// is, the Stack starts with the caller of Wrap.
func Wrap(err error) error {
	return WrapSkip(err, 1)
}

// New returns a new error that includes the Stack.
func New(s string) error {
	return WrapSkip(errors.New(s), 1)
}

// Newf formats and returns a new error that includes the Stack.
func Newf(format string, args ...interface{}) error {
	return WrapSkip(fmt.Errorf(format, args...), 1)
}

type hasUnderlying interface {
	Underlying() error
}

// Underlying returns all the underlying errors by iteratively checking if the
// error has an Underlying error. If e is nil, the returned slice will be nil.
func Underlying(e error) []error {
	if e == nil {
		return nil
	}

	var errs []error
	for {
		if e == nil {
			return errs
		}
		errs = append(errs, e)

		if eh, ok := e.(hasUnderlying); ok {
			e = eh.Underlying()
		} else {
			e = nil
		}
	}
}

// Matcher defines the interface to check if an error matches an expectation.
type Matcher interface {
	Match(e error) bool
}

type equals struct {
	error error
}

func (e *equals) Match(other error) bool {
	return e.error == other
}

// Equals returns a Matcher to check if an error equals the given error.
func Equals(e error) Matcher {
	return &equals{e}
}

// HasUnderlying returns true if any of the underlying errors satisfy the given
// Matcher.
func HasUnderlying(e error, m Matcher) bool {
	for _, o := range Underlying(e) {
		if m.Match(o) {
			return true
		}
	}
	return false
}

// MatcherFunc allows using a function as a Matcher. For example to use
// os.IsNotExist as a matcher.
type MatcherFunc func(err error) bool

// Match calls the underlying function.
func (f MatcherFunc) Match(err error) bool {
	return f(err)
}
