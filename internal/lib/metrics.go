package lib

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	metricsNamespace = "franz"
)

const (
	LabelChannelName = "channel_name"
	LabelChannelID   = "channel_id"
)

type HTTPMetrics struct {
	// httpRequestsInFlight
	// httpRequestDuration
	// httpRequestsTotal
	// httpRequestSize
	// httpResponseSize
}

type AppMetrics struct {
	MessagesSentTotal *prometheus.CounterVec
}

// NewAppMetrics Returns default custom app metrics
func NewAppMetrics(reg prometheus.Registerer) *AppMetrics {
	factory := promauto.With(reg)
	return &AppMetrics{
		MessagesSentTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name:      "messages_sent_total",
				Help:      "Messages sent.",
				Namespace: metricsNamespace,
			},
			[]string{LabelChannelName, LabelChannelID},
		),
	}
}
