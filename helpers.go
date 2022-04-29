package skyffel_kafka

func newConfig() Config {
	c := Config{}
	c.KafkaConfig = make(map[string]string)
	return c
}
