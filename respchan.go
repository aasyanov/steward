package steward

import "sync"

// errRespPool recycles buffered error response channels for scheduler API calls.
var errRespPool = sync.Pool{
	New: func() any {
		return make(chan error, 1)
	},
}

func borrowErrResp() chan error {
	return errRespPool.Get().(chan error)
}

func returnErrResp(ch chan error) {
	errRespPool.Put(ch)
}
