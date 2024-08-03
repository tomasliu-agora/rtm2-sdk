# 目标

本文档帮助基于image/Dockerfile提供便捷的快速Demo开发指导

# 流程

## 安装golang

可以预先下载amd64的Golang包，以1.18.3的golang版本为例

```
rm -rf /usr/local/go
tar -C /usr/local -xzf go1.18.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go version
```

当`go version`返回1.18.3说明golang安装完毕

 ## 设置golang

 需要为当前rtm2的仓库设置NOPROXY等参数

```
go env -w GONOPROXY="github.com/tomasliu-agora/*"
go env -w GONOSUMDB="github.com/tomasliu-agora/*"
go env -w GOPRIVATE="github.com/tomasliu-agora/*"
go env -w GOPROXY=https://goproxy.cn,direct
```

## 创建新golang项目

以goDemo为例

```
mkdir goDemo
cd goDemo
go mod init goDemo
go get github.com/tomasliu-agora/rtm2-sdk
```

如果`go get`无报错，则说明go配置正确且已经将rtm2-sdk下载成功

## 开发业务代码

```go
package main

import (
        "time"
        "go.uber.org/zap"
        "go.uber.org/zap/zapcore"
        "context"
        "github.com/tomasliu-agora/rtm2/v2"
        sdk "github.com/tomasliu-agora/rtm2-sdk"
)

const testAppId = "xxxx" //填写你的appId

func main() {
        ctx, cancel := context.WithCancel(context.Background())
        defer func() {
                cancel()
                time.Sleep(3 * time.Second)
        }()
        lcfg := zap.NewDevelopmentConfig()
        lcfg.Level.SetLevel(zapcore.DebugLevel)
        lg, _ := lcfg.Build()
        config := rtm2.RTMConfig{
                Appid:  testAppId,
                UserId: "userId", // 填写你的vid
                Logger: lg,
        }
        errChan := make(chan error, 10)
        client := sdk.CreateRTM2Client(ctx, config, errChan)
        client.SetParameters(map[string]interface{}{"golang_sidecar_path": "/app"}) // 填写rtm2-wrapper所在的路径
        client.Login("")
        defer client.Logout()
}
```

# 编译并启动

```
go mod tidy
go build
./goDemo
```

当发现日志中输出：`connected	{"endpoint": "127.0.0.1:7001"}`时，说明基本功能正常

# 日志管理

- 默认rtm支持zap库，可以直接传入*zap.Logger
- `RTMConfig`中可以设置FilePath
  - 如果不设置FilePath：
    - 底层C++进程将不输出日志
    - Golang层会自动创建Logger，默认输出在`stdout`中
  - 如果设置FilePath：
    - 如果设置的路径为文件，如：`/tmp/rtm.log`
      - Golang层会自动将日志写入到`/tmp/rtm.go.log`中
      - C++层会将日志写入`/tmp/rtm.log`
    - 如果设置的路径为目录，如: `/tmp/agora`
      - Golang层会自动将日志写入`/tmp/agora/rtm.go.log`
      - C++层会将日志写入`/tmp/agora/rtm.log`