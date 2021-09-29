package band

import (
	"github.com/pkg/errors"

	"github.com/brocaar/chirpstack-network-server/v3/internal/config"
	"github.com/brocaar/lorawan"
	loraband "github.com/brocaar/lorawan/band"
)

var band loraband.Band

// Setup sets up the band with the given configuration.
/*
设置频段、中继器、下行空中停留时间。如果配置额外的通道，
dwellTime: 停留时间
Repeater：中继器
强制中继器兼容
如果可能使用中继器，请将此标志设置为true

*/
func Setup(c config.Config) error {
	dwellTime := lorawan.DwellTimeNoLimit
	if c.NetworkServer.Band.DownlinkDwellTime400ms {
		dwellTime = lorawan.DwellTime400ms
	}

	bandConfig, err := loraband.GetConfig(c.NetworkServer.Band.Name, c.NetworkServer.Band.RepeaterCompatible, dwellTime)
	if err != nil {
		return errors.Wrap(err, "get band config error")
	}
	for _, c := range config.C.NetworkServer.NetworkSettings.ExtraChannels {
		if err := bandConfig.AddChannel(c.Frequency, c.MinDR, c.MaxDR); err != nil {
			return errors.Wrap(err, "add channel error")
		}
	}
	band = bandConfig
	return nil
}

// Band returns the configured band.
func Band() loraband.Band {
	return band
}
