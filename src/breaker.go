package governance

import (
	"sync"
	"time"
)

// 熔断器配置
type Config struct {
	FailThreshold int   `toml:"fail_threshold"` // 失败阈值
	SuccThreshold int   `toml:"succ_threshold"` // 成功阈值
	OpenTimeout   int64 `toml:"open_timeout"`   // 熔断状态置为打开状态的时间阈值，超过此时间将状态置为半打开状态
}

// 熔断状态
type BreakerStatus int

const (
	CloseStatus BreakerStatus = iota
	HalfOpenStatus
	OpenStatus
)

// rpc资源
type RPC struct {
	Status    BreakerStatus // 当前熔断状态
	FailCount int           // 失败次数
	SuccCount int           // 成功次数
	OpenTime  int64         // 熔断状态置为打开时的时间
}

// 熔断器
type Breaker struct {
	Config *Config
	sync.Mutex
	R map[string]*RPC
}

// 初始化熔断器
func InitBreaker(config *Config) *Breaker {
	breaker := &Breaker{
		Config: config,
		R:      make(map[string]*RPC),
	}

	// 启动定时器，定时将rpc资源的熔断状态从打开置为半打开
	go autoHalfOpen(breaker)

	return breaker
}

// 自动rpc资源的熔断状态由打开置为半打开
func autoHalfOpen(breaker *Breaker) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	nowTime := time.Now().Unix()
	for {
		select {
		case <-ticker.C:
			for r, v := range breaker.R {
				if v.Status == OpenStatus && v.OpenTime+breaker.Config.OpenTimeout > nowTime {
					breaker.Lock()
					breaker.R[r] = &RPC{
						Status:    HalfOpenStatus,
						FailCount: 0,
						SuccCount: 0,
						OpenTime:  0,
					}
					breaker.Unlock()
				}
			}
		}
	}
}

func (rpc *RPC) isHalfOpen() bool {
	return rpc.Status == HalfOpenStatus
}

func (rpc *RPC) isOpen() bool {
	return rpc.Status == OpenStatus
}

func (rpc *RPC) isClose() bool {
	return rpc.Status == CloseStatus
}

// 获取rpc资源熔断状态
func (breaker *Breaker) getStatus(r string) BreakerStatus {
	if v, ok := breaker.R[r]; ok {
		return v.Status
	}

	return CloseStatus
}

// 设置rpc资源的熔断状态为打开
func setOpenStatus(rpc *RPC) {
	rpc = &RPC{
		Status:    OpenStatus,
		FailCount: 0,
		SuccCount: 0,
		OpenTime:  time.Now().Unix(),
	}
}

// 调用rpc资源r失败
func (breaker *Breaker) setFail(r string) {
	breaker.Lock()
	defer breaker.Unlock()

	if v, ok := breaker.R[r]; ok {
		/*
		 * 1.rpc资源的熔断状态处于半打开时，只要有失败，就置为打开
		 * 2.rpc资源的熔断状态处于关闭时，当失败次数超过阈值，则置为打开
		 */
		if v.isHalfOpen() {
			setOpenStatus(breaker.R[r])
		} else if v.isClose() {
			v.FailCount++
			if v.FailCount >= breaker.Config.FailThreshold {
				setOpenStatus(breaker.R[r])
			}
		}
	} else {
		breaker.R[r] = &RPC{}
		// 当失败阈值为1时，直接将rpc资源的熔断状态置为打开
		if breaker.Config.FailThreshold == 1 {
			setOpenStatus(breaker.R[r])
		} else {
			breaker.R[r].FailCount = 1
		}
	}
}

// 调用rpc资源r成功
func (breaker *Breaker) setSucc(r string) {
	breaker.Lock()
	defer breaker.Unlock()

	/*
	 * 当rpc资源的熔断状态处于半打开时，若成功次数超过成功阈值，则置为关闭
	 */
	if v, ok := breaker.R[r]; ok {
		if v.isHalfOpen() {
			v.SuccCount++
			if v.SuccCount >= breaker.Config.SuccThreshold {
				breaker.R[r] = &RPC{
					Status:    CloseStatus,
					FailCount: 0,
					SuccCount: 0,
					OpenTime:  0,
				}
			}
		}
	}
}
