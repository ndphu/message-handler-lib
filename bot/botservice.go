package bot

import (
	"errors"
	"github.com/ndphu/message-handler-lib/broker"
	"log"
)

const ActionSendText string = "skype:bot:message:sendText"
const ActionSendImage string = "skype:bot:message:sendImage"
const ActionReactMessage string = "skype:bot:message:react"
const ActionDeleteMessage string = "skype:bot:message:delete"
const ActionListConversation string = "skype:bot:conversation:list"

type RemoteService struct {
	BotId string `json:"id"`
}

func (b *RemoteService) SendText(target, message string) error {
	req := &broker.RpcRequest{
		Method: ActionSendText,
		Args:   []string{target, message},
	}
	return b.parseVoidResponse(req)
}

func (b *RemoteService) SendImage(target, image string) error {
	req := &broker.RpcRequest{
		Method: ActionSendImage,
		Args:   []string{target, image},
	}
	return b.parseVoidResponse(req)
}

func (b*RemoteService) DeleteMessage(target, messageId string) error {
	req := &broker.RpcRequest{
		Method: ActionDeleteMessage,
		Args:   []string{target, messageId},
	}
	return b.parseVoidResponse(req)
}

func (b *RemoteService) ReactMessage(target, image string) error {
	req := &broker.RpcRequest{
		Method: ActionReactMessage,
		Args:   []string{target, image},
	}
	return b.parseVoidResponse(req)
}

func (b *RemoteService) parseVoidResponse(req *broker.RpcRequest) error {
	res, err := b.doRpc(req)
	if err != nil {
		return err
	}
	if res.Error != "" {
		return errors.New(res.Error)
	}
	return nil
}

func (b *RemoteService) doRpc(req *broker.RpcRequest) (*broker.RpcResponse, error) {
	client, err := broker.NewRpcClient(b.BotId)
	if err != nil {
		log.Println("Fail to create RPC client by error", err.Error())
		return nil, err
	}
	return client.SendAndReceive(req)
}
