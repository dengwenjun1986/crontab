package master

import "net/http"

type ApiServer struct {
	httpServer *http.Server
}
