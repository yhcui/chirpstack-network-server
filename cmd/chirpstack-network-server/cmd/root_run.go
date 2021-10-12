package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"

	"github.com/brocaar/chirpstack-api/go/v3/nc"
	"github.com/brocaar/chirpstack-network-server/v3/internal/adr"
	"github.com/brocaar/chirpstack-network-server/v3/internal/api/ns"
	roamingapi "github.com/brocaar/chirpstack-network-server/v3/internal/api/roaming"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/applicationserver"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/controller"
	gwbackend "github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway/amqp"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway/azureiothub"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway/gcppubsub"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway/mqtt"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/joinserver"
	"github.com/brocaar/chirpstack-network-server/v3/internal/band"
	"github.com/brocaar/chirpstack-network-server/v3/internal/config"
	"github.com/brocaar/chirpstack-network-server/v3/internal/downlink"
	"github.com/brocaar/chirpstack-network-server/v3/internal/gateway"
	"github.com/brocaar/chirpstack-network-server/v3/internal/monitoring"
	"github.com/brocaar/chirpstack-network-server/v3/internal/roaming"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	"github.com/brocaar/chirpstack-network-server/v3/internal/uplink"
)

func run(cmd *cobra.Command, args []string) error {
	//创建建一个新的lora服务
	var server = new(uplink.Server)

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return errors.Wrap(err, "could not create cpu profile file")
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			return errors.Wrap(err, "could not start cpu profile")
		}
		defer pprof.StopCPUProfile()
	}

	tasks := []func() error{
		setLogLevel,             // 根据配置文件设置日志级别
		setSyslog,               // system log. linux
		setGRPCResolver,         // grpc resolver
		setupBand,               // 设置工作频段相关信息bandName等
		setRXParameters,         // 设置RX参数。RX2DR和RX2Frequency
		printStartMessage,       // 打印启动日志
		setupMonitoring,         // 设置时区与监控信息
		enableUplinkChannels,    // 设置启用的上行通道
		setupStorage,            // 启动存储相关的.连接redis和PostgreSQL
		setGatewayBackend,       // 设置网关服务。此处之基站与NS的协议服务。如:MQTT
		setupApplicationServer,  // 设置AS连接池
		setupADR,                // adr插件, https://zhuanlan.zhihu.com/p/113746989
		setupJoinServer,         // join server。 设备入网时进行验证的服务.
		setupNetworkController,  // todo 提供管理无线网络的能力，它通过MAC命令获取或设置End Nodes的网络参数和射频性能
		setupUplink,             // 设置上行的入网、重新入网、数据传输配置数据
		setupDownlink,           // 设置下行的入网、广播、传用、数据配置数据
		setupNetworkServerAPI,   // 启动grpc协议,外部组件通过它来调度mac命令
		setupRoaming,            // 漫游服务. https://blog.csdn.net/iotisan/article/details/102612404
		setupGateways,           // StatsPacketChan -- 处理网关上传的状态上报数据
		startLoRaServer(server), // 启动loRaServer 处理上下行数据
		startQueueScheduler,     // 好像是下行消息是广播还是单发某个设备的相关代码
	}
	// 循环启动每一个任务
	for _, t := range tasks {
		if err := t(); err != nil {
			log.Fatal(err)
		}
	}

	sigChan := make(chan os.Signal)
	exitChan := make(chan struct{})
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	log.WithField("signal", <-sigChan).Info("signal received")
	go func() {
		log.Warning("stopping chirpstack-network-server")
		if err := server.Stop(); err != nil {
			log.Fatal(err)
		}
		if err := gateway.Stop(); err != nil {
			log.Fatal(err)
		}
		exitChan <- struct{}{}
	}()
	select {
	case <-exitChan:
	case s := <-sigChan:
		log.WithField("signal", s).Info("signal received, stopping immediately")
	}

	return nil
}

func setGRPCResolver() error {
	resolver.SetDefaultScheme(config.C.General.GRPCDefaultResolverScheme)
	return nil
}

func setupBand() error {
	if err := band.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup band error")
	}

	return nil
}

func setRXParameters() error {
	defaults := band.Band().GetDefaults()
	// RX2数据速率.当设置为-1时，默认RX2数据速率将用于配置的LoRaWAN频带
	if config.C.NetworkServer.NetworkSettings.RX2DR == -1 {
		config.C.NetworkServer.NetworkSettings.RX2DR = defaults.RX2DataRate
	}
	// RX2频率 当设置为-1时，将使用默认的RX2频率。
	if config.C.NetworkServer.NetworkSettings.RX2Frequency == -1 {
		config.C.NetworkServer.NetworkSettings.RX2Frequency = int64(defaults.RX2Frequency)
	}

	return nil
}

// TODO: cleanup and put in Setup functions.
func setupMonitoring() error {
	// setup timezone
	var err error
	if config.C.Metrics.Timezone == "" {
		err = storage.SetTimeLocation(config.C.NetworkServer.Gateway.Stats.Timezone)
	} else {
		err = storage.SetTimeLocation(config.C.Metrics.Timezone)
	}
	if err != nil {
		return errors.Wrap(err, "set time location error")
	}

	if err := monitoring.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup metrics error")
	}

	return nil
}

func setLogLevel() error {
	log.SetLevel(log.Level(uint8(config.C.General.LogLevel)))
	return nil
}

func printStartMessage() error {
	log.WithFields(log.Fields{
		"version": version,
		"net_id":  config.C.NetworkServer.NetID.String(),
		"band":    config.C.NetworkServer.Band.Name,
		"docs":    "https://www.chirpstack.io/",
	}).Info("starting ChirpStack Network Server")
	return nil
}

func enableUplinkChannels() error {
	if len(config.C.NetworkServer.NetworkSettings.EnabledUplinkChannels) == 0 {
		return nil
	}

	log.Info("disabling all channels")
	for _, c := range band.Band().GetEnabledUplinkChannelIndices() {
		if err := band.Band().DisableUplinkChannelIndex(c); err != nil {
			return errors.Wrap(err, "disable uplink channel error")
		}
	}

	log.WithField("channels", config.C.NetworkServer.NetworkSettings.EnabledUplinkChannels).Info("enabling channels")
	for _, c := range config.C.NetworkServer.NetworkSettings.EnabledUplinkChannels {
		if err := band.Band().EnableUplinkChannelIndex(c); err != nil {
			return errors.Wrap(err, "enable uplink channel error")
		}
	}

	return nil
}

func setupStorage() error {
	if err := storage.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup storage error")
	}
	return nil
}

/*
ADR(Adaptive Data Rate)速率自适应是LoRaWAN的核心功能之
开启ADR功能后，NS服务器可以接管每一个终端的通信速率及发射功率，
使得终端功耗最优以及通信速率最高，从而实现网络容量的最大化以及低功耗终端寿命的提升

ADR功能需要NS服务器来动态管理网络内所有节点的通信速率及功率。然而官方协议中只明确给出了MAC命令的帧格式，
对于NS侧的ADR算法实现（即何时，以什么为标准，将终端速率及功率到怎样的水平）Semtech并没有具体给出

ADR算法不适用于移动终端，若终端节点位置相对于网关位置经常发生改变，
那么基于最近数据包信号质量而选择的数据速率可能会和新环境不匹配而导致通信丢包。

*/
func setupADR() error {
	if err := adr.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup adr error")
	}
	return nil
}

func setGatewayBackend() error {
	var err error
	var gw gwbackend.Gateway

	switch config.C.NetworkServer.Gateway.Backend.Type {
	case "mqtt":
		gw, err = mqtt.NewBackend(
			config.C,
		)
	case "amqp":
		gw, err = amqp.NewBackend(config.C)
	case "gcp_pub_sub":
		gw, err = gcppubsub.NewBackend(config.C)
	case "azure_iot_hub":
		gw, err = azureiothub.NewBackend(config.C)
	default:
		return fmt.Errorf("unexpected gateway backend type: %s", config.C.NetworkServer.Gateway.Backend.Type)
	}

	if err != nil {
		return errors.Wrap(err, "gateway-backend setup failed")
	}

	gwbackend.SetBackend(gw)
	return nil
}

func setupApplicationServer() error {
	if err := applicationserver.Setup(); err != nil {
		return errors.Wrap(err, "application-server setup error")
	}
	return nil
}

func setupJoinServer() error {
	if err := joinserver.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup join-server backend error")
	}
	return nil
}

func setupNetworkController() error {
	// TODO: move this logic to controller.Setup function
	if config.C.NetworkController.Server != "" {
		// setup network-controller client
		log.WithFields(log.Fields{
			"server":   config.C.NetworkController.Server,
			"ca-cert":  config.C.NetworkController.CACert,
			"tls-cert": config.C.NetworkController.TLSCert,
			"tls-key":  config.C.NetworkController.TLSKey,
		}).Info("connecting to network-controller")
		ncDialOptions := []grpc.DialOption{
			grpc.WithBalancerName(roundrobin.Name),
		}
		if config.C.NetworkController.TLSCert != "" && config.C.NetworkController.TLSKey != "" {
			ncDialOptions = append(ncDialOptions, grpc.WithTransportCredentials(
				mustGetTransportCredentials(config.C.NetworkController.TLSCert, config.C.NetworkController.TLSKey, config.C.NetworkController.CACert, false),
			))
		} else {
			ncDialOptions = append(ncDialOptions, grpc.WithInsecure())
		}
		ncConn, err := grpc.Dial(config.C.NetworkController.Server, ncDialOptions...)
		if err != nil {
			return errors.Wrap(err, "network-controller dial error")
		}
		ncClient := nc.NewNetworkControllerServiceClient(ncConn)
		controller.SetClient(ncClient)
	}

	return nil
}

func setupUplink() error {
	if err := uplink.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup link error")
	}
	return nil
}

func setupDownlink() error {
	if err := downlink.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup downlink error")
	}
	return nil
}

func setupNetworkServerAPI() error {
	if err := ns.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup network-server api error")
	}

	return nil
}

/*
大型跨国公司可能在多个国家拥有设施，不同的LoraWan运营商覆盖该领土。
漫游使跨国公司能够与其本国境内的运营商建立单一的商业关系
*/
func setupRoaming() error {
	if err := roamingapi.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup roaming api error")
	}

	if err := roaming.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup roaming error")
	}

	return nil
}

/*
topic为 gateway/+/event/+
通过topic后缀来区分上下行消息 分别为 up（上行） act(下行) stats(上行，状态上报)
通过MHDR.MType来进行区分消息类型.如joinreqeust
*/
func startLoRaServer(server *uplink.Server) func() error {
	return func() error {
		*server = *uplink.NewServer()
		return server.Start()
	}
}

func setupGateways() error {
	if err := gateway.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup gateway error")
	}
	return nil
}

/*
消息可以是单播，也可以是组播。给一个终端设备单独发送消息使用单播，给多个终端设备发送使用组播。
*/
func startQueueScheduler() error {
	log.Info("starting downlink device-queue scheduler")
	// 只处理 class b和class c的设备
	go downlink.DeviceQueueSchedulerLoop()

	log.Info("starting multicast scheduler")
	go downlink.MulticastQueueSchedulerLoop()

	return nil
}

func mustGetTransportCredentials(tlsCert, tlsKey, caCert string, verifyClientCert bool) credentials.TransportCredentials {
	cert, err := tls.LoadX509KeyPair(tlsCert, tlsKey)
	if err != nil {
		log.WithFields(log.Fields{
			"cert": tlsCert,
			"key":  tlsKey,
		}).Fatalf("load key-pair error: %s", err)
	}

	var caCertPool *x509.CertPool
	if caCert != "" {
		rawCaCert, err := ioutil.ReadFile(caCert)
		if err != nil {
			log.WithField("ca", caCert).Fatalf("load ca cert error: %s", err)
		}

		caCertPool = x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(rawCaCert) {
			log.WithField("ca_cert", caCert).Fatal("append ca certificate error")
		}
	}

	if verifyClientCert {
		return credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    caCertPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		})
	}

	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	})
}
