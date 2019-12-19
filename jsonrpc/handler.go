package jsonrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	"golang.org/x/xerrors"
)

type rpcHandler struct {
	paramReceivers []reflect.Type
	nParams        int

	receiver    reflect.Value
	handlerFunc reflect.Value

	hasCtx int

	errOut int
	valOut int
}

type handlers map[string]rpcHandler

// Request / response

type request struct {
	Jsonrpc string            `json:"jsonrpc"`
	ID      *int64            `json:"id,omitempty"`
	Method  string            `json:"method"`
	Params  []param           `json:"params"`
	Meta    map[string]string `json:"meta,omitempty"`
}

type respError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *respError) Error() string {
	if e.Code >= -32768 && e.Code <= -32000 {
		return fmt.Sprintf("RPC error (%d): %s", e.Code, e.Message)
	}
	return e.Message
}

type response struct {
	Jsonrpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	ID      int64       `json:"id"`
	Error   *respError  `json:"error,omitempty"`
}

// Register

func (h handlers) register(namespace string, r interface{}) {
	val := reflect.ValueOf(r)
	//TODO: expect ptr

	for i := 0; i < val.NumMethod(); i++ {
		method := val.Type().Method(i)

		funcType := method.Func.Type()
		hasCtx := 0
		if funcType.NumIn() >= 2 && funcType.In(1) == contextType {
			hasCtx = 1
		}

		ins := funcType.NumIn() - 1 - hasCtx
		recvs := make([]reflect.Type, ins)
		for i := 0; i < ins; i++ {
			recvs[i] = method.Type.In(i + 1 + hasCtx)
		}

		valOut, errOut, _ := processFuncOut(funcType)

		h[namespace+"."+method.Name] = rpcHandler{
			paramReceivers: recvs,
			nParams:        ins,

			handlerFunc: method.Func,
			receiver:    val,

			hasCtx: hasCtx,

			errOut: errOut,
			valOut: valOut,
		}
	}
}

// Handle

type rpcErrFunc func(w func(func(io.Writer)), req *request, code int, err error)
type chanOut func(reflect.Value) interface{}

func (h handlers) handleReader(ctx context.Context, r io.Reader, w io.Writer, rpcError rpcErrFunc) {
	wf := func(cb func(io.Writer)) {
		cb(w)
	}

	var req request
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		rpcError(wf, &req, rpcParseError, xerrors.Errorf("unmarshaling request: %w", err))
		return
	}

	h.handle(ctx, req, wf, rpcError, func(bool) {}, nil)
}

func doCall(methodName string, f reflect.Value, params []reflect.Value) (out []reflect.Value, err error) {
	defer func() {
		if i := recover(); i != nil {
			err = xerrors.Errorf("panic in rpc method '%s': %s", methodName, i)
			log.Error(err)
		}
	}()

	out = f.Call(params)
	return out, nil
}

func (handlers) getSpan(ctx context.Context, req request) (context.Context, *trace.Span) {
	if req.Meta == nil {
		return ctx, nil
	}
	if eSC, ok := req.Meta["SpanContext"]; ok {
		bSC := make([]byte, base64.StdEncoding.DecodedLen(len(eSC)))
		_, err := base64.StdEncoding.Decode(bSC, []byte(eSC))
		if err != nil {
			log.Errorf("SpanContext: decode", "error", err)
			return ctx, nil
		}
		sc, ok := propagation.FromBinary(bSC)
		if !ok {
			log.Errorf("SpanContext: could not create span", "data", bSC)
			return ctx, nil
		}
		ctx, span := trace.StartSpanWithRemoteParent(ctx, "api.handle", sc)
		span.AddAttributes(trace.StringAttribute("method", req.Method))
		return ctx, span
	}
	return ctx, nil
}

func (h handlers) handle(ctx context.Context, req request, w func(func(io.Writer)), rpcError rpcErrFunc, done func(keepCtx bool), chOut chanOut) {
	ctx, span := h.getSpan(ctx, req)
	defer span.End()

	handler, ok := h[req.Method]
	if !ok {
		rpcError(w, &req, rpcMethodNotFound, fmt.Errorf("method '%s' not found", req.Method))
		done(false)
		return
	}

	if len(req.Params) != handler.nParams {
		rpcError(w, &req, rpcInvalidParams, fmt.Errorf("wrong param count"))
		done(false)
		return
	}

	outCh := handler.valOut != -1 && handler.handlerFunc.Type().Out(handler.valOut).Kind() == reflect.Chan
	defer done(outCh)

	if chOut == nil && outCh {
		rpcError(w, &req, rpcMethodNotFound, fmt.Errorf("method '%s' not supported in this mode (no out channel support)", req.Method))
		return
	}

	callParams := make([]reflect.Value, 1+handler.hasCtx+handler.nParams)
	callParams[0] = handler.receiver
	if handler.hasCtx == 1 {
		callParams[1] = reflect.ValueOf(ctx)
	}

	for i := 0; i < handler.nParams; i++ {
		rp := reflect.New(handler.paramReceivers[i])
		if err := json.NewDecoder(bytes.NewReader(req.Params[i].data)).Decode(rp.Interface()); err != nil {
			rpcError(w, &req, rpcParseError, xerrors.Errorf("unmarshaling params for '%s': %w", handler.handlerFunc, err))
			return
		}

		callParams[i+1+handler.hasCtx] = reflect.ValueOf(rp.Elem().Interface())
	}

	///////////////////

	callResult, err := doCall(req.Method, handler.handlerFunc, callParams)
	if err != nil {
		rpcError(w, &req, 0, xerrors.Errorf("fatal error calling '%s': %w", req.Method, err))
		return
	}
	if req.ID == nil {
		return // notification
	}

	///////////////////

	resp := response{
		Jsonrpc: "2.0",
		ID:      *req.ID,
	}

	if handler.errOut != -1 {
		err := callResult[handler.errOut].Interface()
		if err != nil {
			log.Warnf("error in RPC call to '%s': %+v", req.Method, err)
			resp.Error = &respError{
				Code:    1,
				Message: err.(error).Error(),
			}
		}
	}
	if handler.valOut != -1 {
		resp.Result = callResult[handler.valOut].Interface()
	}

	w(func(w io.Writer) {
		if resp.Result != nil && reflect.TypeOf(resp.Result).Kind() == reflect.Chan {
			// this must happen in the writer callback, otherwise we may start sending
			// channel messages before we send this response

			//noinspection GoNilness // already checked above
			resp.Result = chOut(callResult[handler.valOut])
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Error(err)
			return
		}
	})
}
