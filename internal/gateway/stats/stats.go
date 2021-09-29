package stats

import (
	"context"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-api/go/v3/as"
	"github.com/brocaar/chirpstack-api/go/v3/common"
	"github.com/brocaar/chirpstack-api/go/v3/gw"
	"github.com/brocaar/chirpstack-network-server/v3/internal/backend/gateway"
	"github.com/brocaar/chirpstack-network-server/v3/internal/band"
	"github.com/brocaar/chirpstack-network-server/v3/internal/helpers"
	"github.com/brocaar/chirpstack-network-server/v3/internal/logging"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	"github.com/brocaar/lorawan"
	loraband "github.com/brocaar/lorawan/band"
)

var ErrAbort = errors.New("abort")

/*
网关上传状态的上下文
*/
type statsContext struct {
	ctx          context.Context
	gatewayID    lorawan.EUI64
	gatewayStats gw.GatewayStats
	gatewayMeta  storage.GatewayMeta
}

var tasks = []func(*statsContext) error{
	updateGatewayState,               // 修改网关 经度、纬度、高度、上传时间等信息
	getGatewayMeta,                   // 获取网关元数据并缓存
	handleGatewayConfigurationUpdate, // 向网关发送更新网关配置信息。如果配置网关配置及Concentratord信息,且状态上传的并非最新版本，则进行更新MQTT发送
	forwardGatewayStats,              // 将网关状态信息转发给AS
}

// Handle handles the gateway stats
func Handle(ctx context.Context, stats gw.GatewayStats) error {
	gatewayID := helpers.GetGatewayID(&stats)

	sctx := statsContext{
		ctx:          ctx,
		gatewayID:    gatewayID,
		gatewayStats: stats,
	}

	for _, t := range tasks {
		if err := t(&sctx); err != nil {
			if err == ErrAbort {
				return nil
			}
			return err
		}
	}

	return nil
}

func updateGatewayState(ctx *statsContext) error {
	if err := storage.UpdateGatewayState(
		ctx.ctx,
		storage.DB(),
		ctx.gatewayID,
		ctx.gatewayStats.GetLocation().GetLatitude(),
		ctx.gatewayStats.GetLocation().GetLongitude(),
		ctx.gatewayStats.GetLocation().GetAltitude(),
	); err != nil {
		return errors.Wrap(err, "update gateway state error")
	}

	return nil
}

func getGatewayMeta(ctx *statsContext) error {

	gw, err := storage.GetAndCacheGatewayMeta(ctx.ctx, storage.DB(), ctx.gatewayID)
	if err != nil {
		return errors.Wrap(err, "get gateway meta error")
	}
	ctx.gatewayMeta = gw

	return nil
}

func handleGatewayConfigurationUpdate(ctx *statsContext) error {
	// no gateway-profile configured
	if ctx.gatewayMeta.GatewayProfileID == nil {
		log.WithFields(log.Fields{
			"gateway_id": ctx.gatewayMeta.GatewayID,
			"ctx_id":     ctx.ctx.Value(logging.ContextIDKey),
		}).Debug("gateway-profile is not set, skipping configuration update")
		return nil
	}

	// not using concentratord
	if ctx.gatewayStats.GetMetaData()["concentratord_version"] == "" {
		log.WithFields(log.Fields{
			"gateway_id": ctx.gatewayMeta.GatewayID,
		}).Debug("gatway does not support configuration updates")
		return nil
	}

	// get gateway-profile
	gwProfile, err := storage.GetGatewayProfile(ctx.ctx, storage.DB(), *ctx.gatewayMeta.GatewayProfileID)
	if err != nil {
		return errors.Wrap(err, "get gateway-profile error")
	}

	// compare gateway-profile config version with stats config version
	if gwProfile.GetVersion() == ctx.gatewayStats.ConfigVersion || gwProfile.GetVersion() == ctx.gatewayStats.GetMetaData()["config_version"] {
		log.WithFields(log.Fields{
			"gateway_id": ctx.gatewayMeta.GatewayID,
			"version":    ctx.gatewayStats.ConfigVersion,
			"ctx_id":     ctx.ctx.Value(logging.ContextIDKey),
		}).Debug("gateway configuration is up-to-date")
		return nil
	}

	configPacket := gw.GatewayConfiguration{
		GatewayId:     ctx.gatewayMeta.GatewayID[:],
		StatsInterval: ptypes.DurationProto(gwProfile.StatsInterval),
		Version:       gwProfile.GetVersion(),
	}

	for _, i := range gwProfile.Channels {
		c, err := band.Band().GetUplinkChannel(int(i))
		if err != nil {
			return errors.Wrap(err, "get channel error")
		}

		gwC := gw.ChannelConfiguration{
			Frequency:  uint32(c.Frequency),
			Modulation: common.Modulation_LORA,
		}

		modConfig := gw.LoRaModulationConfig{}

		for drI := c.MaxDR; drI >= c.MinDR; drI-- {
			dr, err := band.Band().GetDataRate(drI)
			if err != nil {
				return errors.Wrap(err, "get data-rate error")
			}

			// skip non-LoRa modulations (e.g. LR-FHSS) and non 125 kHz data-rates
			if dr.Modulation != loraband.LoRaModulation || dr.Bandwidth != 125 {
				continue
			}

			modConfig.SpreadingFactors = append(modConfig.SpreadingFactors, uint32(dr.SpreadFactor))
			modConfig.Bandwidth = uint32(dr.Bandwidth)
		}

		gwC.ModulationConfig = &gw.ChannelConfiguration_LoraModulationConfig{
			LoraModulationConfig: &modConfig,
		}

		configPacket.Channels = append(configPacket.Channels, &gwC)
	}

	for _, c := range gwProfile.ExtraChannels {
		gwC := gw.ChannelConfiguration{
			Frequency: uint32(c.Frequency),
		}

		switch loraband.Modulation(c.Modulation) {
		case loraband.LoRaModulation:
			gwC.Modulation = common.Modulation_LORA
			modConfig := gw.LoRaModulationConfig{
				Bandwidth: uint32(c.Bandwidth),
			}

			for _, sf := range c.SpreadingFactors {
				modConfig.SpreadingFactors = append(modConfig.SpreadingFactors, uint32(sf))
			}

			gwC.ModulationConfig = &gw.ChannelConfiguration_LoraModulationConfig{
				LoraModulationConfig: &modConfig,
			}
		case loraband.FSKModulation:
			gwC.Modulation = common.Modulation_FSK
			modConfig := gw.FSKModulationConfig{
				Bandwidth: uint32(c.Bandwidth),
				Bitrate:   uint32(c.Bitrate),
			}

			gwC.ModulationConfig = &gw.ChannelConfiguration_FskModulationConfig{
				FskModulationConfig: &modConfig,
			}
		}

		configPacket.Channels = append(configPacket.Channels, &gwC)
	}

	if err := gateway.Backend().SendGatewayConfigPacket(configPacket); err != nil {
		return errors.Wrap(err, "send gateway-configuration packet error")
	}

	return nil
}

func forwardGatewayStats(ctx *statsContext) error {
	rp, err := storage.GetRoutingProfile(ctx.ctx, storage.DB(), ctx.gatewayMeta.RoutingProfileID)
	if err != nil {
		return errors.Wrap(err, "get routing-profile error")
	}

	asClient, err := rp.GetApplicationServerClient()
	if err != nil {
		return errors.Wrap(err, "get application-server client error")
	}

	_, err = asClient.HandleGatewayStats(ctx.ctx, &as.HandleGatewayStatsRequest{
		GatewayId:             ctx.gatewayStats.GatewayId,
		StatsId:               ctx.gatewayStats.StatsId,
		Time:                  ctx.gatewayStats.Time,
		Location:              ctx.gatewayStats.Location,
		RxPacketsReceived:     ctx.gatewayStats.RxPacketsReceived,
		RxPacketsReceivedOk:   ctx.gatewayStats.RxPacketsReceivedOk,
		TxPacketsReceived:     ctx.gatewayStats.TxPacketsReceived,
		TxPacketsEmitted:      ctx.gatewayStats.TxPacketsEmitted,
		Metadata:              ctx.gatewayStats.MetaData,
		TxPacketsPerFrequency: ctx.gatewayStats.TxPacketsPerFrequency,
		RxPacketsPerFrequency: ctx.gatewayStats.RxPacketsPerFrequency,
		TxPacketsPerDr:        perModulationToPerDR(false, ctx.gatewayStats.TxPacketsPerModulation),
		RxPacketsPerDr:        perModulationToPerDR(true, ctx.gatewayStats.RxPacketsPerModulation),
		TxPacketsPerStatus:    ctx.gatewayStats.TxPacketsPerStatus,
	})
	if err != nil {
		return errors.Wrap(err, "handle gateway stats error")
	}

	return nil
}

func perModulationToPerDR(uplink bool, items []*gw.PerModulationCount) map[uint32]uint32 {
	out := make(map[uint32]uint32)
	b := band.Band()

	for _, item := range items {
		mod := item.GetModulation()
		var dr loraband.DataRate

		if modParams := mod.GetLora(); modParams != nil {
			dr = loraband.DataRate{
				Modulation:   loraband.LoRaModulation,
				SpreadFactor: int(modParams.SpreadingFactor),
				Bandwidth:    int(modParams.Bandwidth),
			}
		}

		if modParams := mod.GetFsk(); modParams != nil {
			dr = loraband.DataRate{
				Modulation: loraband.FSKModulation,
				BitRate:    int(modParams.Datarate),
			}
		}

		if modParams := mod.GetLrFhss(); modParams != nil {
			dr = loraband.DataRate{
				Modulation:           loraband.LRFHSSModulation,
				CodingRate:           modParams.CodeRate,
				OccupiedChannelWidth: int(modParams.OperatingChannelWidth),
			}
		}

		if dr, err := b.GetDataRateIndex(uplink, dr); err == nil {
			out[uint32(dr)] = out[uint32(dr)] + item.Count
		} else {
			log.WithError(err).Error("gateway/stats: convert modulation parameters to data-rate error")
		}
	}

	return out
}
