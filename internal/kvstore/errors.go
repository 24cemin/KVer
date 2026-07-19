package kvstore

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrWrongType      = errors.New("wrong type operation against a key holding the wrong kind of value")
	ErrOutOfRange     = errors.New("index out of range")
	ErrEmptyList      = errors.New("list is empty")
	ErrUnknownCommand = errors.New("unknown command")
	ErrInvalidArgs    = errors.New("wrong number or type of arguments")
)
