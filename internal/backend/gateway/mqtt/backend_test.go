package mqtt

import (
	"context"
	"fmt"
	"testing"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/brocaar/chirpstack-api/go/v3/gw"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway/marshaler"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	"github.com/brocaar/chirpstack-network-server/v3/internal/test"
	"github.com/brocaar/lorawan"
)

type BackendTestSuite struct {
	suite.Suite

	backend    gateway.Gateway
	mqttClient paho.Client
}

func (ts *BackendTestSuite) SetupSuite() {
	assert := require.New(ts.T())

	conf := test.GetConfig()
	assert.NoError(storage.Setup(conf))

	opts := paho.NewClientOptions().
		AddBroker(conf.NetworkServer.Gateway.Backend.MQTT.Server).
		SetUsername(conf.NetworkServer.Gateway.Backend.MQTT.Username).
		SetPassword(conf.NetworkServer.Gateway.Backend.MQTT.Password)
	ts.mqttClient = paho.NewClient(opts)
	token := ts.mqttClient.Connect()
	token.Wait()
	assert.NoError(token.Error())

	var err error
	ts.backend, err = NewBackend(conf)
	assert.NoError(err)

	ts.backend.(*Backend).setGatewayMarshaler(lorawan.EUI64{1, 2, 3, 4, 5, 6, 7, 8}, marshaler.Protobuf)
}

func (ts *BackendTestSuite) TearDownSuite() {
	assert := require.New(ts.T())

	assert.NoError(ts.backend.Close())
}

func (ts *BackendTestSuite) SetupTest() {
	storage.RedisClient().FlushAll(context.Background())
}

func TestUplinkFrameCui(t *testing.T) {

	uplinkFrame := gw.UplinkFrame{
		PhyPayload: []byte{1, 2, 3, 4},
		TxInfo: &gw.UplinkTXInfo{
			Frequency: 868100000,
		},
		RxInfo: &gw.UplinkRXInfo{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}

	b, _ := proto.Marshal(&uplinkFrame)
	uplinkFrame.XXX_sizecache = 0
	uplinkFrame.TxInfo.XXX_sizecache = 0
	uplinkFrame.RxInfo.XXX_sizecache = 0
	fmt.Println(b)

	var uplinkFrame2 gw.UplinkFrame
	proto.Unmarshal(b, &uplinkFrame2)
	fmt.Println(uplinkFrame2)

}

func (ts *BackendTestSuite) TestUplinkFrame() {
	assert := require.New(ts.T())

	uplinkFrame := gw.UplinkFrame{
		PhyPayload: []byte{1, 2, 3, 4},
		TxInfo: &gw.UplinkTXInfo{
			Frequency: 868100000,
		},
		RxInfo: &gw.UplinkRXInfo{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}

	b, err := proto.Marshal(&uplinkFrame)
	assert.NoError(err)
	uplinkFrame.XXX_sizecache = 0
	uplinkFrame.TxInfo.XXX_sizecache = 0
	uplinkFrame.RxInfo.XXX_sizecache = 0
	fmt.Println(b)
	token := ts.mqttClient.Publish("gateway/0102030405060708/event/up", 0, false, b)
	token.Wait()
	assert.NoError(token.Error())

	receivedUplink := <-ts.backend.RXPacketChan()
	assert.EqualValues(uplinkFrame, receivedUplink)
}

func (ts *BackendTestSuite) TestGatewayStats() {
	assert := require.New(ts.T())

	gatewayStats := gw.GatewayStats{
		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}

	b, err := proto.Marshal(&gatewayStats)
	assert.NoError(err)
	gatewayStats.XXX_sizecache = 0

	token := ts.mqttClient.Publish("gateway/0102030405060708/event/stats", 0, false, b)
	token.Wait()
	assert.NoError(token.Error())

	receivedStats := <-ts.backend.StatsPacketChan()
	assert.EqualValues(gatewayStats, receivedStats)
}

func (ts *BackendTestSuite) TestDownlinkTXAck() {
	assert := require.New(ts.T())

	downlinkTXAck := gw.DownlinkTXAck{
		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	b, err := proto.Marshal(&downlinkTXAck)
	assert.NoError(err)

	token := ts.mqttClient.Publish("gateway/0102030405060708/event/ack", 0, false, b)
	token.Wait()
	assert.NoError(token.Error())

	receivedAck := <-ts.backend.DownlinkTXAckChan()
	if !proto.Equal(&downlinkTXAck, &receivedAck) {
		assert.Equal(downlinkTXAck, receivedAck)
	}
}

func (ts *BackendTestSuite) TestSendDownlinkFrame() {
	assert := require.New(ts.T())

	downlinkFrameChan := make(chan gw.DownlinkFrame)
	token := ts.mqttClient.Subscribe("gateway/+/command/down", 0, func(c paho.Client, msg paho.Message) {
		var pl gw.DownlinkFrame
		if err := proto.Unmarshal(msg.Payload(), &pl); err != nil {
			panic(err)
		}

		downlinkFrameChan <- pl
	})
	token.Wait()
	assert.NoError(token.Error())

	ts.T().Run("hybrid", func(t *testing.T) {
		assert := require.New(t)
		ts.backend.(*Backend).downMode = "hybrid"

		assert.NoError(ts.backend.SendTXPacket(gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
					TxInfo:     &gw.DownlinkTXInfo{},
				},
			},
		}))

		downlink := <-downlinkFrameChan
		proto.Equal(&downlink, &gw.DownlinkFrame{
			GatewayId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
			PhyPayload: []byte{1, 2, 3},
			TxInfo: &gw.DownlinkTXInfo{
				GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
				},
			},
		})
	})

	ts.T().Run("multi_only", func(t *testing.T) {
		assert := require.New(t)
		ts.backend.(*Backend).downMode = "multi_only"

		assert.NoError(ts.backend.SendTXPacket(gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
					TxInfo:     &gw.DownlinkTXInfo{},
				},
			},
		}))

		downlink := <-downlinkFrameChan
		proto.Equal(&downlink, &gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
				},
			},
		})
	})

	ts.T().Run("legacy", func(t *testing.T) {
		assert := require.New(t)
		ts.backend.(*Backend).downMode = "legacy"

		assert.NoError(ts.backend.SendTXPacket(gw.DownlinkFrame{
			GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			Items: []*gw.DownlinkFrameItem{
				{
					PhyPayload: []byte{1, 2, 3},
					TxInfo:     &gw.DownlinkTXInfo{},
				},
			},
		}))

		downlink := <-downlinkFrameChan
		proto.Equal(&downlink, &gw.DownlinkFrame{
			PhyPayload: []byte{1, 2, 3},
			TxInfo: &gw.DownlinkTXInfo{
				GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			},
		})
	})
}

func (ts *BackendTestSuite) TestSendGatewayConfiguration() {
	assert := require.New(ts.T())

	gatewayConfigChan := make(chan gw.GatewayConfiguration)
	token := ts.mqttClient.Subscribe("gateway/+/command/config", 0, func(c paho.Client, msg paho.Message) {
		var pl gw.GatewayConfiguration
		if err := proto.Unmarshal(msg.Payload(), &pl); err != nil {
			panic(err)
		}
		gatewayConfigChan <- pl
	})
	token.Wait()
	assert.NoError(token.Error())

	gatewayConfig := gw.GatewayConfiguration{
		GatewayId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Version:   "1.2.3",
	}
	assert.NoError(ts.backend.SendGatewayConfigPacket(gatewayConfig))
	gatewayConfig.XXX_sizecache = 0

	configReceived := <-gatewayConfigChan
	assert.Equal(gatewayConfig, configReceived)
}

func TestBackend(t *testing.T) {
	suite.Run(t, new(BackendTestSuite))
}
