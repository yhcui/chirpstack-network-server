package uplink

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-api/go/v3/gw"
	"github.com/brocaar/chirpstack-network-server/v3/internal/band"
	"github.com/brocaar/chirpstack-network-server/v3/internal/helpers"
	"github.com/brocaar/chirpstack-network-server/v3/internal/models"
	"github.com/brocaar/chirpstack-network-server/v3/internal/storage"
	"github.com/brocaar/lorawan"
)

// Templates used for generating Redis keys
const (
	CollectKeyTempl     = "lora:ns:rx:collect:%s:%s"
	CollectLockKeyTempl = "lora:ns:rx:collect:%s:%s:lock"
)

// collectAndCallOnce collects the package, sleeps the configured duration and
// calls the callback only once with a slice of packets, sorted by signal
// strength (strongest at index 0). This method exists since multiple gateways
// are able to receive the same packet, but the packet needs to processed
// only once.
// It is safe to collect the same packet received by the same gateway twice.
// Since the underlying storage type is a set, the result will always be a
// unique set per gateway MAC and packet MIC.
/*
主要功能：将gw.UplinkFrame 转为 models.RXPacket,并调用 callback
描述
收集数据包，休眠配置的持续时间，并使用数据包片段仅调用回调一次，按信号强度排序（在索引0处最强）。
这种方法的存在是因为多个网关能够接收相同的数据包，但是数据包只需要处理一次。

收集同一网关接收的同一数据包两次是安全的。由于底层存储类型是一个集合，因此每个网关MAC和数据包MIC的结果总是唯一的集合。
*/
func collectAndCallOnce(rxPacket gw.UplinkFrame, callback func(packet models.RXPacket) error) error {
	phyKey := hex.EncodeToString(rxPacket.PhyPayload)
	txInfoB, err := proto.Marshal(rxPacket.TxInfo)
	if err != nil {
		return errors.Wrap(err, "marshal protobuf error")
	}
	txInfoHEX := hex.EncodeToString(txInfoB)

	// 根据TxInfo和PhyPalyload 生成16进制唯一标识并生成rediskey,Txinfo中没有gatewayID
	key := storage.GetRedisKey(CollectKeyTempl, txInfoHEX, phyKey)
	lockKey := storage.GetRedisKey(CollectLockKeyTempl, txInfoHEX, phyKey)

	deduplicationTTL := deduplicationDelay * 2 // 缓存过期时间?
	if deduplicationTTL < time.Millisecond*200 {
		deduplicationTTL = time.Millisecond * 200
	}
	// 将rxPacket存放缓存中,相同key只会存放一次 -- deduplicationTTL:缓存超时时长
	if err := collectAndCallOncePut(key, deduplicationTTL, rxPacket); err != nil {
		return err
	}
	// 加lock -- deduplicationTTL:缓存超时时长
	if locked, err := collectAndCallOnceLocked(lockKey, deduplicationTTL); err != nil || locked {
		// when locked == true, err == nil
		return err
	}

	// wait the configured amount of time, more packets might be received
	// from other gateways
	// 休眠 -- 非常重要
	time.Sleep(deduplicationDelay)

	// collect all packets from the set 同时删除缓存key及相关数据
	payloads, err := collectAndCallOnceCollect(key)
	if err != nil {
		return errors.Wrap(err, "get deduplication set members error")
	}
	// 此处为rxPacket
	if len(payloads) == 0 {
		return errors.New("zero items in collect set")
	}

	var out models.RXPacket
	for i, b := range payloads {
		var uplinkFrame gw.UplinkFrame
		if err := proto.Unmarshal(b, &uplinkFrame); err != nil {
			return errors.Wrap(err, "unmarshal uplink frame error")
		}

		if uplinkFrame.TxInfo == nil {
			log.Warning("tx-info of uplink frame is empty, skipping")
			continue
		}

		if uplinkFrame.RxInfo == nil {
			log.Warning("rx-info of uplink frame is empty, skipping")
			continue
		}

		if i == 0 {
			var phy lorawan.PHYPayload
			if err := phy.UnmarshalBinary(uplinkFrame.PhyPayload); err != nil {
				return errors.Wrap(err, "unmarshal phypayload error")
			}

			out.PHYPayload = phy

			dr, err := helpers.GetDataRateIndex(true, uplinkFrame.TxInfo, band.Band())
			if err != nil {
				return errors.Wrap(err, "get data-rate index error")
			}
			out.DR = dr
		}

		out.TXInfo = uplinkFrame.TxInfo
		out.RXInfoSet = append(out.RXInfoSet, uplinkFrame.RxInfo)
		out.GatewayIsPrivate = make(map[lorawan.EUI64]bool)
		out.GatewayServiceProfile = make(map[lorawan.EUI64]uuid.UUID)
	}

	return callback(out)
}

func collectAndCallOncePut(key string, ttl time.Duration, rxPacket gw.UplinkFrame) error {
	b, err := proto.Marshal(&rxPacket)
	if err != nil {
		return errors.Wrap(err, "marshal uplink frame error")
	}
	// 启用pipline管道模式的client
	pipe := storage.RedisClient().TxPipeline()

	// Sadd 命令将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略
	pipe.SAdd(context.Background(), key, b)

	// 设置过期时间
	pipe.PExpire(context.Background(), key, ttl)

	_, err = pipe.Exec(context.Background())
	if err != nil {
		return errors.Wrap(err, "add uplink frame to set error")
	}

	return nil
}

func collectAndCallOnceLocked(key string, ttl time.Duration) (bool, error) {
	// this way we can set a really low DeduplicationDelay for testing, without
	// the risk that the set already expired in redis on read
	deduplicationTTL := deduplicationDelay * 2
	if deduplicationTTL < time.Millisecond*200 {
		deduplicationTTL = time.Millisecond * 200
	}

	set, err := storage.RedisClient().SetNX(context.Background(), key, "lock", ttl).Result()
	if err != nil {
		return false, errors.Wrap(err, "acquire deduplication lock error")
	}

	// Set is true when we were able to set the lock, we return true if it
	// was already locked.
	return !set, nil
}

func collectAndCallOnceCollect(key string) ([][]byte, error) {
	pipe := storage.RedisClient().Pipeline()
	val := pipe.SMembers(context.Background(), key) // 命令返回集合中的所有的成员。 不存在的集合 key 被视为空集合
	pipe.Del(context.Background(), key)

	if _, err := pipe.Exec(context.Background()); err != nil {
		return nil, errors.Wrap(err, "get set members error")
	}

	var out [][]byte
	vals := val.Val()

	for i := range vals {
		out = append(out, []byte(vals[i]))
	}

	return out, nil
}
