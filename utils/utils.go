package utils

import (
	"fmt"
	"github.com/ndphu/message-handler-lib/broker"
	"regexp"
	"strings"
)

var InsideSpacesRegex = regexp.MustCompile(`[\s\p{Zs}]{2,}`)

func SendText(workerId, threadId, result string) (*broker.RpcResponse, error) {
	rpc, err := broker.NewRpcClient(workerId)
	if err != nil {
		return nil, nil
	}
	request := &broker.RpcRequest{
		Method: "sendText",
		Args:   []string{threadId, WrapAsPreformatted(result)},
	}
	return rpc.SendAndReceive(request)
}

func SendError(workerId, threadId string, err error) (*broker.RpcResponse, error) {
	return SendText(workerId, threadId, fmt.Sprintf("Error:\n%s\n", err.Error()))
}

func SendImage(workerId, target, url string) (*broker.RpcResponse, error) {
	c, err := broker.NewRpcClient(workerId)
	if err != nil {
		return nil, err
	}
	request := &broker.RpcRequest{
		Method: "sendImageFromUrl",
		Args:   []string{target, url},
	}

	return c.SendAndReceive(request)
}

func WrapAsPreformatted(message string) string {
	return fmt.Sprintf("<pre raw_pre=\"{code}\" raw_post=\"{code}\">%s</pre>", message)
}

func RemoveBlankSpaces(input string) string {
	final := strings.TrimSpace(input)
	return InsideSpacesRegex.ReplaceAllString(final, " ")
}
