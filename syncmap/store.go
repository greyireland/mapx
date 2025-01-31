package syncmap

type PubSubStore interface {
	Close() error
	Set(key, value string) error
	Get(key string) (string, error)
	Del(key string) error
	Keys(pattern string, limit int, withvalues bool) ([]string, []string, error)
	Publish(topic string, msg string) error
	Subscribe(topic string, ch chan string) error
}
