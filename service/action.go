package service

import (
	"errors"
	"time"
)

var ErrorInvalidActionArguments = errors.New("InvalidActionArgument")

type Handler func(args []string) (interface{}, error)

type Action struct {
	Name          string  `json:"name"`
	ArgumentCount int     `json:"argumentCount"`
	Handler       Handler `json:"-"`
}

func (a *Action) ValidateArgs(args []string) error {
	if a.ArgumentCount != len(args) {
		return ErrorInvalidActionArguments
	}
	return nil
}

type ActionContext struct {
	Args []string `json:"args"`
}

type ActionResult map[string]interface{}

func getBasicActions() []Action {
	return []Action{
		{
			Name:          "service:ping",
			ArgumentCount: 0,
			Handler: func(args []string) (interface{}, error) {
				return ActionResult{
					"message": "pong",
				}, nil
			},
		},
		{
			Name:          "service:getTime",
			ArgumentCount: 0,
			Handler: func(args []string) (interface{}, error) {
				return ActionResult{
					"time": time.Now(),
				}, nil
			},
		},
	}
}
