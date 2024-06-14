package rtm2_sdk

import (
	"context"
	"errors"
	"fmt"
	"github.com/tomasliu-agora/rtm2"
	base "github.com/tomasliu-agora/rtm2-base"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"strings"
	"time"
)

type rtmInvoker struct {
	ctx    context.Context
	cancel context.CancelFunc

	callback base.InvokeCallback
	sidecar  *rtmSidecar
	conn     *connection

	errorChan chan<- error

	lg  *zap.Logger
	cli rtm2.RTMClient

	timeout time.Duration
}

func (i *rtmInvoker) onResponse(uri int32, errCode int32, message []byte) error {
	switch uri {
	case UriConnectStateChange:
		event := &base.ConnectionStateChangeEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriMessageEvent:
		event := &base.MessageEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriStreamEvent:
		event := &base.StreamMessageEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriStreamTopicEvent:
		event := &base.StreamTopicEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriStorageChannelEvent:
		event := &base.StorageChannelEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriStorageUserEvent:
		event := &base.StorageUserEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriPresenceEvent:
		event := &base.PresenceEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriLockEvent:
		event := &base.LockEvent{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	case UriTokenPrivilegeExpire:
		event := &base.TokenPrivilegeExpire{}
		err := event.Unmarshal(message)
		if err != nil {
			i.lg.Error("Failed to unmarshal", zap.Error(err))
			return err
		}
		i.callback.OnEvent(event)
	default:
		i.lg.Warn("unknown event", zap.Int32("uri", uri), zap.Int32("errCode", errCode))
	}
	return nil
}

func (i *rtmInvoker) OnReceived(req interface{}) (interface{}, int32, error) {
	rc := make(chan *Header, 1)
	uri := getUriFromReq(req)
	if uri == invalidUri {
		return nil, 0, errors.New("unknown error")
	}
	err := i.conn.SendRequest(generateHeader(uri, req.(Marshalable)), rc)
	if err != nil {
		return nil, 0, err
	}
	resp, err := i.receive(rc)
	if err != nil {
		return nil, 999, err
	} else if len(resp.Message) != 0 {
		return unmarshalResp(uri, resp.ErrCode, resp.Message)
	} else {
		return nil, resp.ErrCode, nil
	}
}

func (i *rtmInvoker) OnAsyncReceived(req interface{}, callback func(interface{}, int32, error)) error {
	rc := make(chan *Header, 1)
	uri := getUriFromReq(req)
	if uri == invalidUri {
		return errors.New("unknown error")
	}
	err := i.conn.SendRequest(generateHeader(uri, req.(Marshalable)), rc)
	if err != nil {
		return err
	}
	go func() {
		resp, rErr := i.receive(rc)
		i.lg.Debug("on async recv", zap.Any("resp", resp))
		if rErr != nil {
			callback(nil, 0, rErr)
		} else if len(resp.Message) != 0 {
			respObj, errCode, mErr := unmarshalResp(uri, resp.ErrCode, resp.Message)
			callback(respObj, errCode, mErr)
		} else {
			callback(nil, resp.ErrCode, nil)
		}
	}()
	return nil
}

func (i *rtmInvoker) receive(rc <-chan *Header) (*Header, error) {
	select {
	case h, ok := <-rc:
		if !ok {
			return nil, ERR_DISCONNECTED
		}
		if h.ErrCode != 0 {
			return nil, rtm2.ErrorFromCode(h.ErrCode)
		}
		return h, nil
	case <-time.After(i.timeout):
		i.lg.Info("timeout")
		return nil, ERR_TIMEOUT
	}
}

func (i *rtmInvoker) SetCallback(callback base.InvokeCallback) {
	i.callback = callback
}

func (i *rtmInvoker) PreLogin() {
	params := i.cli.GetParameters()
	if value, ok := params[kParamSidecarEndpoint]; ok {
		endpoint := value.(string)
		i.conn = NewConnection(i.ctx, i.lg, endpoint, i)
		i.conn.Start()
	} else {
		var (
			port int32
		)
		if value, ok = params[kParamSidecarPort]; ok {
			port, ok = value.(int32)
			if !ok {
				port = DefaultSidecarPort
			}
		} else {
			port = DefaultSidecarPort
		}
		execPath := "."
		if p, ok := params[kParamSidecarPath]; ok {
			execPath = p.(string)
		}
		i.sidecar = createSidecar(i.ctx, i.lg, execPath, port)
		i.conn = NewConnection(i.ctx, i.lg, fmt.Sprintf("127.0.0.1:%d", port), i)
		go i.loop()
		i.conn.Start()
	}
}

func (i *rtmInvoker) PostLogin() {}

func (i *rtmInvoker) PreLogout() {}

func (i *rtmInvoker) PostLogout() {
	i.cancel()
	i.sidecar.Stop()
}

func (i *rtmInvoker) loop() {
	errChan := i.sidecar.Start()
	var err error
	defer func() {
		if err != nil {
			i.errorChan <- err
		}
		i.cancel()
		i.sidecar.Stop()
	}()
	for {
		select {
		case <-i.ctx.Done():
			i.lg.Info("context canceled")
			return
		case err = <-errChan:
			i.lg.Info("sidecar error", zap.Error(err))
			return
		case err = <-i.conn.ErrorChan():
			i.lg.Info("connection error", zap.Error(err))
			return
		}
	}
}

func initLogger(config *rtm2.RTMConfig) {
	if config.Logger != nil {
		return
	}
	lcfg := zap.NewProductionConfig()
	if len(config.FilePath) == 0 {
		lcfg.Level.SetLevel(zapcore.InfoLevel)
		config.Logger, _ = lcfg.Build()
	} else {
		filePath := config.FilePath
		if strings.HasSuffix(filePath, ".log") {
			filePath = filePath[:len(filePath)-4] + ".go.log"
		} else {
			filePath = filePath + "/rtm.go.log"
			config.FilePath = config.FilePath + "/rtm.log"
		}
		l := &lumberjack.Logger{
			Filename:  filePath,
			MaxSize:   50,
			MaxAge:    2,
			LocalTime: true,
			Compress:  true,
		}
		core := zapcore.NewCore(zapcore.NewJSONEncoder(lcfg.EncoderConfig), zapcore.AddSync(zapcore.AddSync(l)), zapcore.DebugLevel)
		config.Logger = zap.New(zapcore.NewTee(core), zap.AddCaller())
	}
}

func CreateRTM2Client(ctx context.Context, config rtm2.RTMConfig, errChan chan<- error) rtm2.RTMClient {
	c, cancel := context.WithCancel(ctx)
	initLogger(&config)
	inv := &rtmInvoker{ctx: c, cancel: cancel, sidecar: nil, lg: config.Logger, errorChan: errChan}
	if config.RequestTimeout <= 0 {
		inv.timeout = 5000 * time.Millisecond
	} else {
		inv.timeout = time.Duration(config.RequestTimeout) * time.Millisecond
	}
	cli := base.CreateRTMClient(ctx, config, inv)
	inv.cli = cli
	return cli
}
