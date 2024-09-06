package domain

type ErrorType int8

const (
	ErrTypeMarshalSS ErrorType = iota
	ErrTypeDb
	ErrTypeNotFound
	ErrTypeVersionExists
	ErrTypeUnauthorized
	ErrTypeInternal
	ErrTypeSchemaInvalid
)

type Error struct {
	errType ErrorType
	message string
}

func NewError(errType ErrorType, message string) *Error {
	return &Error{
		errType: errType,
		message: message,
	}
}

func (e Error) ErrType() ErrorType {
	return e.errType
}

func (e Error) Message() string {
	return e.message
}
