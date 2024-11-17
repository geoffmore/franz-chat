package lib

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// See https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Registry

type PGMetrics struct {
	UsersTotal    prometheus.Gauge
	ChannelsTotal prometheus.Gauge
	MessagesTotal *prometheus.GaugeVec
}

// Returns metrics only used during session-level advisory lock hold
func NewPGMetrics(reg prometheus.Registerer) *PGMetrics {
	// TODO - switch from promauto factory to custom register/unregister
	factory := promauto.With(reg)
	// See https://prometheus.io/docs/practices/naming/ for naming
	return &PGMetrics{
		UsersTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name:      "users_total",
			Help:      "Current users present in app.",
			Namespace: metricsNamespace,
		}),
		ChannelsTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "channels_total",
			Help: "Current channels present in app.",
			// TODO - add channel_name, channel_id as labels
			Namespace: metricsNamespace,
		}),
		MessagesTotal: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "messages_total",
			Help: "Current messages present in app, by channel.",
			// TODO - add channel_name, channel_id as labels
			Namespace: metricsNamespace,
		}, []string{LabelChannelName, LabelChannelID},
		),
	}
	// reg.Register() w/ err on lock acquisition
	// reg.Unregister() on lock release
}
