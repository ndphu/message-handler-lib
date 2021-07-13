package model

import "path"

type SkypeMessage struct {
	Id             string `json:"id"`
	Time           string `json:"composeTime"`
	Content        string `json:"content"`
	ConversationId string `json:"conversationId"`
	MessageType    string `json:"messageType"`
	Type           string `json:"type"`
	Version        string `json:"version"`
	From           string `json:"from"`
	SkypeEditedId  string `json:"skypeeditedid"`
}

type Resource struct {
	Id          string       `json:"id"`
	LastMessage SkypeMessage `json:"lastMessage"`
}

type MessageEvent struct {
	Id           int             `json:"id"`
	ResourceLink string          `json:"resourceLink"`
	ResourceType string          `json:"resourceType"`
	Time         string          `json:"time"`
	Type         string          `json:"type"`
	Resource     MessageResource `json:"resource"`
}

func (e *MessageEvent) GetThreadId() string {
	return path.Base(e.Resource.ConversationLink)
}

func (e *MessageEvent) GetFrom() string {
	return path.Base(e.Resource.From)
}

func (e *MessageEvent) IsTextMessage() bool {
	return e.Type == "EventMessage" && e.ResourceType == "NewMessage" && e.Resource.MessageType == "RichText"
}

func (e *MessageEvent) IsMediaMessage() bool {
	return e.Type == "EventMessage" && e.ResourceType == "NewMessage" && e.Resource.MessageType == "RichText/UriObject"
}

func (e *MessageEvent) GetContent() string {
	return e.Resource.Content
}

func (e *MessageEvent) GetType() string {
	return e.Resource.Type
}

func (e *MessageEvent) ContentType() string {
	return e.Resource.ContentType
}

func (e*MessageEvent) MessageId() string {
	return e.Resource.Id
}

type MessageResource struct {
	Type             string `json:"type"`
	From             string `json:"from"`
	ClientMessageId  string `json:"clientmessageid"`
	Content          string `json:"content"`
	ContentType      string `json:"contenttype"`
	ThreadTopic      string `json:"thread_topic"`
	ConversationLink string `json:"conversationLink"`
	Id               string `json:"id"`
	MessageType      string `json:"messagetype"`
}
