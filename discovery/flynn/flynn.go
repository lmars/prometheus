package flynn

import (
	"time"

	"github.com/flynn/flynn/discoverd/client"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"golang.org/x/net/context"
)

const (
	retryInterval = 15 * time.Second

	metaLabelPrefix = model.MetaLabelPrefix + "flynn_"
	serviceLabel    = metaLabelPrefix + "service"
	appIDLabel      = metaLabelPrefix + "app_id"
	releaseIDLabel  = metaLabelPrefix + "release_id"
	jobIDLabel      = metaLabelPrefix + "job_id"
	jobTypeLabel    = metaLabelPrefix + "job_type"
)

// Discovery retrieves target information from a discoverd server
// and updates them via watches
type Discovery struct {
	services []string
	client   *discoverd.Client
}

// NewDiscovery returns a new Discovery which periodically refreshes its targets
func NewDiscovery(conf *config.FlynnSDConfig) *Discovery {
	return &Discovery{
		services: conf.Services,
		client:   discoverd.NewClient(),
	}
}

// Run implements the TargetProvider interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*config.TargetGroup) {
	for _, name := range d.services {
		go d.watch(name, ctx, ch)
	}
	<-ctx.Done()
}

func (d *Discovery) watch(name string, ctx context.Context, ch chan<- []*config.TargetGroup) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		events := make(chan *discoverd.Event)
		stream, err := d.client.Service(name).Watch(events)
		if err != nil {
			log.Errorf("error watching %s service: %s", name, err)
			time.Sleep(retryInterval)
			continue
		}
		defer stream.Close()

		for {
			select {
			case event, ok := <-events:
				if !ok {
					log.Errorf("error watching %s service: %s", name, stream.Err())
					time.Sleep(retryInterval)
					continue
				}

				var tg *config.TargetGroup
				switch event.Kind {
				case discoverd.EventKindUp:
					target := model.LabelSet{
						model.AddressLabel: model.LabelValue(event.Instance.Addr),
					}
					if path, ok := event.Instance.Meta["PROMETHEUS_METRICS_PATH"]; ok {
						target[model.MetricsPathLabel] = model.LabelValue(path)
					}
					tg = &config.TargetGroup{
						Source: event.Instance.ID,
						Labels: model.LabelSet{
							serviceLabel:   model.LabelValue(event.Service),
							appIDLabel:     model.LabelValue(event.Instance.Meta["FLYNN_APP_ID"]),
							releaseIDLabel: model.LabelValue(event.Instance.Meta["FLYNN_RELEASE_ID"]),
							jobIDLabel:     model.LabelValue(event.Instance.Meta["FLYNN_JOB_ID"]),
							jobTypeLabel:   model.LabelValue(event.Instance.Meta["FLYNN_PROCESS_TYPE"]),
						},
						Targets: []model.LabelSet{target},
					}
				case discoverd.EventKindDown:
					tg = &config.TargetGroup{
						Source: event.Instance.ID,
					}
				}

				if tg != nil {
					select {
					case ch <- []*config.TargetGroup{tg}:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}
}
