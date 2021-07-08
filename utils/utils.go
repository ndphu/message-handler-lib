package utils

func GetQueueAndExchangeName(serviceName, workerId, consumerId string) (string, string) {
	exchange := "/worker/" + workerId + "/textMessages"
	queueName := "/message-handler/" + serviceName + "/queue-" + consumerId
	return exchange, queueName
}
