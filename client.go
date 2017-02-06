package iceberg

func NewClient(addr string) (Stream, error) {
	return newStream(), nil
}
