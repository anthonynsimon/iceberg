package iceberg

func NewClient(addr string) (Iceberg, error) {
	return newIcequeue(), nil
}
