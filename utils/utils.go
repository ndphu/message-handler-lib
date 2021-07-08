package utils


func GetQueueAndExchangeName(workerId string, consumerId string) (string, string) {
	exchange := "/worker/" + workerId + "/textMessages"
	queueName := "/message-handler/example-message-handler-" + consumerId
	return exchange, queueName
}

