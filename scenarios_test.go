package steward_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aasyanov/steward"
)

type cameraConfig struct {
	URL     string
	Bitrate int
}

type cameraUnit struct {
	url       string
	bitrate   int
	frames    atomic.Int64
	recording atomic.Bool
}

func (c *cameraUnit) ID() string { return c.url }
func (c *cameraUnit) Start(ctx context.Context) error {
	go c.run(ctx)
	return nil
}

func (c *cameraUnit) run(ctx context.Context) {
	c.recording.Store(true)
	defer c.recording.Store(false)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.frames.Add(1)
		}
	}
}
func (c *cameraUnit) Stop(context.Context) error { return nil }

func TestScenario_CameraRecorderPool(t *testing.T) {
	var mu sync.Mutex
	recorders := map[string]*cameraUnit{}

	makeHooks := func(bitrate int) (steward.BuildFunc[string, cameraConfig], steward.EqualFunc[cameraConfig]) {
		return func(_ context.Context, id string, cfg cameraConfig) (steward.Unit, error) {
				b := cfg.Bitrate
				if b == 0 {
					b = bitrate
				}
				c := &cameraUnit{url: cfg.URL, bitrate: b}
				mu.Lock()
				recorders[cfg.URL] = c
				mu.Unlock()
				_ = id
				return c, nil
			},
			func(a, b cameraConfig) bool {
				return a.URL == b.URL && a.Bitrate == b.Bitrate
			}
	}

	build, equal := makeHooks(4000)
	set := steward.NewSet[string, cameraConfig](build, equal)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]cameraConfig{
		"cam-1": {URL: "rtsp://10.0.0.1/stream1"},
		"cam-2": {URL: "rtsp://10.0.0.2/stream1"},
	})
	waitUntil(t, time.Second, func() bool {
		return set.Running("cam-1") && set.Running("cam-2")
	})

	set.Reconcile(map[string]cameraConfig{
		"cam-1": {URL: "rtsp://10.0.0.1/stream1"},
		"cam-2": {URL: "rtsp://10.0.0.2/stream2"},
		"cam-3": {URL: "rtsp://10.0.0.3/stream1"},
	})
	waitUntil(t, time.Second, func() bool { return set.Running("cam-3") })

	mu.Lock()
	old := recorders["rtsp://10.0.0.2/stream1"]
	newRec := recorders["rtsp://10.0.0.2/stream2"]
	mu.Unlock()
	if old != nil && old.recording.Load() {
		t.Fatal("old recorder still running")
	}
	if newRec == nil {
		t.Fatal("new recorder missing")
	}
	waitUntil(t, time.Second, func() bool { return newRec.frames.Load() > 0 })

	build, equal = makeHooks(8000)
	set.Replace(build, equal, map[string]cameraConfig{
		"cam-1": {URL: "rtsp://10.0.0.1/stream1", Bitrate: 8000},
		"cam-2": {URL: "rtsp://10.0.0.2/stream2", Bitrate: 8000},
		"cam-3": {URL: "rtsp://10.0.0.3/stream1", Bitrate: 8000},
	})
	waitUntil(t, time.Second, func() bool {
		return set.Running("cam-1") && set.Running("cam-2") && set.Running("cam-3")
	})
}

type pollerConfig struct {
	IP       string
	Register int
}

type pollerUnit struct {
	ip    string
	reads atomic.Int64
}

func (p *pollerUnit) ID() string { return p.ip }
func (p *pollerUnit) Start(ctx context.Context) error {
	go p.run(ctx)
	return nil
}

func (p *pollerUnit) run(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.reads.Add(1)
		}
	}
}
func (p *pollerUnit) Stop(context.Context) error { return nil }

func TestScenario_NetworkPollerFleet(t *testing.T) {
	pollers := sync.Map{}
	set := steward.NewSet[string, pollerConfig](
		func(_ context.Context,
		id string, cfg pollerConfig) (steward.Unit, error) {
			p := &pollerUnit{ip: cfg.IP}
			pollers.Store(cfg.IP, p)
			_ = id
			return p, nil
		},
		func(a, b pollerConfig) bool { return a == b },)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	desired := map[string]pollerConfig{
		"plc-a": {IP: "192.168.10.10", Register: 40001},
		"plc-b": {IP: "192.168.10.11", Register: 40001},
	}
	set.Reconcile(desired)
	waitUntil(t, time.Second, func() bool {
		return set.Running("plc-a") && set.Running("plc-b")
	})

	set.Reconcile(map[string]pollerConfig{"plc-a": desired["plc-a"]})
	waitUntil(t, time.Second, func() bool { return !set.Running("plc-b") })

	// Removed poller must stop ticking (allow stop + supervisor drain).
	waitUntil(t, time.Second, func() bool {
		v, ok := pollers.Load("192.168.10.11")
		if !ok {
			return true
		}
		p := v.(*pollerUnit)
		reads := p.reads.Load()
		time.Sleep(30 * time.Millisecond)
		return p.reads.Load() == reads
	})
}

func TestScenario_MessageConsumers(t *testing.T) {
	var processed atomic.Int64
	type subCfg struct {
		Topic   string
		GroupID string
	}

	set := steward.NewSet[string, subCfg](
		func(_ context.Context,
		id string, _ subCfg) (steward.Unit, error) {
			return steward.RunUnit(id, func(ctx context.Context) error {
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						processed.Add(1)
					}
				}
			}), nil
		},
		func(a, b subCfg) bool { return a == b },)
	set.Start(context.Background())
	defer set.Stop(context.Background())

	set.Reconcile(map[string]subCfg{
		"s1": {Topic: "telemetry", GroupID: "ingest"},
		"s2": {Topic: "alarms", GroupID: "ingest"},
	})
	waitUntil(t, time.Second, func() bool {
		return set.Running("s1") && set.Running("s2")
	})

	before := processed.Load()
	set.Reconcile(map[string]subCfg{
		"s1": {Topic: "telemetry", GroupID: "ingest"},
		"s3": {Topic: "metrics", GroupID: "ingest"},
	})
	waitUntil(t, time.Second, func() bool {
		return set.Running("s3") && !set.Running("s2")
	})

	time.Sleep(30 * time.Millisecond)
	if processed.Load() <= before {
		t.Fatal("expected continued processing")
	}
}
