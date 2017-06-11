package logging

type LogFunc func(msg string)

func NewWrapperLogFunc(logger LogFunc) LogFunc {
	return func(msg string) {
		if logger != nil {
			logger(msg)
		}
	}
}