package loraserver

import (
	"io"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"

	log "github.com/Sirupsen/logrus"
	"github.com/brocaar/lorawan"
)

// JSONRPCHandler implements a http.Handler compatible JSON-RPC handler.
type JSONRPCHandler struct {
	server *rpc.Server
}

// NewJSONRPCHandler creates a new JSONRPCHandler.
func NewJSONRPCHandler(srvcs ...interface{}) (http.Handler, error) {
	s := rpc.NewServer()
	for _, srvc := range srvcs {
		if err := s.Register(srvc); err != nil {
			return nil, err
		}
	}
	return &JSONRPCHandler{s}, nil
}

// ServeHTTP implements the http.Handler interface.
func (h *JSONRPCHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn := struct {
		io.Writer
		io.ReadCloser
	}{w, r.Body}

	if err := h.server.ServeRequest(jsonrpc.NewServerCodec(conn)); err != nil {
		log.Errorf("could not handle json-rpc request: %s", err)
	}
}

// API defines the RPC API.
type API struct {
	ctx Context
}

// NewAPI creates a new API.
func NewAPI(ctx Context) *API {
	return &API{
		ctx: ctx,
	}
}

// GetApplication returns the Application for the given AppEUI.
func (a *API) GetApplication(appEUI lorawan.EUI64, app *Application) error {
	var err error
	*app, err = GetApplication(a.ctx.DB, appEUI)
	return err
}

// CreateApplication creates the given application.
func (a *API) CreateApplication(app Application, appEUI *lorawan.EUI64) error {
	if err := CreateApplication(a.ctx.DB, app); err != nil {
		return err
	}
	*appEUI = app.AppEUI
	return nil
}

// UpdateApplication updates the given Application.
func (a *API) UpdateApplication(app Application, appEUI *lorawan.EUI64) error {
	if err := UpdateApplication(a.ctx.DB, app); err != nil {
		return err
	}
	*appEUI = app.AppEUI
	return nil
}

// DeleteApplication deletes the application for the given AppEUI.
func (a *API) DeleteApplication(appEUI lorawan.EUI64, deletedAppEUI *lorawan.EUI64) error {
	if err := DeleteApplication(a.ctx.DB, appEUI); err != nil {
		return err
	}
	*deletedAppEUI = appEUI
	return nil
}

// GetNode returns the Node for the given DevEUI.
func (a *API) GetNode(devEUI lorawan.EUI64, node *Node) error {
	var err error
	*node, err = GetNode(a.ctx.DB, devEUI)
	return err
}

// CreateNode creates the given Node.
func (a *API) CreateNode(node Node, devEUI *lorawan.EUI64) error {
	if err := CreateNode(a.ctx.DB, node); err != nil {
		return err
	}
	*devEUI = node.DevEUI
	return nil
}

// UpdateNode updatest the given Node.
func (a *API) UpdateNode(node Node, devEUI *lorawan.EUI64) error {
	if err := UpdateNode(a.ctx.DB, node); err != nil {
		return err
	}
	*devEUI = node.DevEUI
	return nil
}

// DeleteNode deletes the node matching the given DevEUI.
func (a *API) DeleteNode(devEUI lorawan.EUI64, deletedDevEUI *lorawan.EUI64) error {
	if err := DeleteNode(a.ctx.DB, devEUI); err != nil {
		return err
	}
	*deletedDevEUI = devEUI
	return nil
}