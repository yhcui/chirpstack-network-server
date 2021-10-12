package gateway

import (
	"math/rand"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/brocaar/chirpstack-network-server/v3/internal/band"
	"github.com/brocaar/chirpstack-network-server/v3/internal/config"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	loraband "github.com/brocaar/lorawan/band"
)

// BySignal implements sort.Interface for []gw.UplinkRXInfo based on signal strength.
type BySignal []storage.DeviceGatewayRXInfo

// Len returns the number of elements.
func (s BySignal) Len() int {
	return len(s)
}

// Swap swaps i and j.
func (s BySignal) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less returns if i is greater than j (reverse sorting).
func (s BySignal) Less(i, j int) bool {
	// Sort on RSSI when SNR is equal (or for FSK).
	if s[i].LoRaSNR == s[j].LoRaSNR {
		return s[i].RSSI > s[j].RSSI
	}

	return s[i].LoRaSNR > s[j].LoRaSNR
}

// SelectDownlinkGateway returns, given a slice of DeviceGatewayRXInfo
// elements the gateway (as a DeviceGatewayRXInfo) to use for downlink.
// In the current implementation it will sort the given slice based on SNR / RSSI,
// and return:
//  * A random item from the elements with an SNR > minSNR
//  * The first item of the sorted slice (failing the above)
/*
在当前的实现中，它将根据SNR/RSSI对给定的切片进行排序，并返回：
1 信噪比>最小信噪比的元素中的随机项
2 已排序切片的第一项（ 1 操作失败）
*/
func SelectDownlinkGateway(minSNRMargin float64, rxDR int, rxInfo []storage.DeviceGatewayRXInfo) (storage.DeviceGatewayRXInfo, error) {
	if len(rxInfo) == 0 {
		return storage.DeviceGatewayRXInfo{}, errors.New("device gateway rx-info slice is empty")
	}
	dr, err := band.Band().GetDataRate(rxDR)
	if err != nil {
		return storage.DeviceGatewayRXInfo{}, errors.Wrap(err, "get data-rate error")
	}

	// Sort by SNR.
	sort.Sort(BySignal(rxInfo))

	// This builds a slice of items where the (Required SNR - RX SNR) > minMargin.
	var newRxInfo []storage.DeviceGatewayRXInfo
	for i := range rxInfo {
		// 这里"-"是一个减
		if dr.Modulation == loraband.LoRaModulation && (rxInfo[i].LoRaSNR-config.SpreadFactorToRequiredSNRTable[dr.SpreadFactor]) >= minSNRMargin {
			newRxInfo = append(newRxInfo, rxInfo[i])
		}
	}

	// Return first element from sorted slice failing the above.
	if len(newRxInfo) == 0 {
		return rxInfo[0], nil
	}

	// Return random item from SNR > minSNR slice.
	rand.Seed(time.Now().UnixNano())
	return newRxInfo[rand.Intn(len(newRxInfo))], nil
}
