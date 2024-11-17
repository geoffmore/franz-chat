package lib

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

// See https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Registry

// NOTE - each of Gauge, Counter, Histogram, Summary are all collectors, so they can be registered/unregistered programmatically already

// franzDBStatsCollector inspired by https://github.com/prometheus/client_golang/blob/v1.20.5/prometheus/collectors/dbstats_collector.go#L40
type franzDBStatsCollector struct {
	conn          *pgxpool.Conn
	ctx           context.Context
	usersTotal    *prometheus.Desc
	channelsTotal *prometheus.Desc
	messagesTotal *prometheus.Desc
}

// Collect implements prometheus.Collector
func (c *franzDBStatsCollector) Collect(ch chan<- prometheus.Metric) {
	channelMessageCounts := c.getChannelMessagesTotal(c.ctx, c.conn)

	ch <- prometheus.MustNewConstMetric(c.usersTotal, prometheus.GaugeValue, c.getUsersTotal(c.ctx, c.conn))
	ch <- prometheus.MustNewConstMetric(c.channelsTotal, prometheus.GaugeValue, c.getChannelsTotal(c.ctx, c.conn))

	for k, v := range channelMessageCounts {
		ch <- prometheus.MustNewConstMetric(c.messagesTotal, prometheus.GaugeValue, v, k)
	}
}

// Describe implements prometheus.Collector
func (c *franzDBStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.usersTotal
	ch <- c.channelsTotal
	ch <- c.messagesTotal
}

func NewFranzDBStatsCollector(ctx context.Context, conn *pgxpool.Conn) prometheus.Collector {
	fqName := func(name string) string {
		return fmt.Sprintf("%s_%s", metricsNamespace, name)
	}
	return &franzDBStatsCollector{
		conn: conn,
		ctx:  ctx,
		usersTotal: prometheus.NewDesc(
			fqName("users_total"),
			"Current users present in app.",
			nil, nil,
		),
		channelsTotal: prometheus.NewDesc(
			fqName("channels_total"),
			"Current channels present in app.",
			nil, nil,
		),
		messagesTotal: prometheus.NewDesc(
			fqName("messages_total"),
			"Current messages present in app, by channel.",
			[]string{LabelChannelID}, nil,
		),
	}
}

// This generates a prometheus metric of a count of users on the database
// This remains private because I only want code calling this when the advisory lock is held
func (c *franzDBStatsCollector) getUsersTotal(ctx context.Context, conn *pgxpool.Conn) float64 {
	var i int
	row := conn.QueryRow(ctx, "SELECT count(*) FROM users;")

	if err := row.Scan(&i); HandlePGError(err, nil) != nil {
		log.Err(err) // TODO - determine how to log this error without embedding a logger in this package
	}
	return float64(i)
}

func (c *franzDBStatsCollector) getChannelsTotal(ctx context.Context, conn *pgxpool.Conn) float64 {
	var i int
	row := conn.QueryRow(ctx, "SELECT count(*) FROM channels;")

	if err := row.Scan(&i); HandlePGError(err, nil) != nil {
		log.Err(err)
	}
	return float64(i)
}

func (c *franzDBStatsCollector) getChannelMessagesTotal(ctx context.Context, conn *pgxpool.Conn) map[string]float64 {
	_, _ = ctx, conn
	return map[string]float64{"TODO": 54321}
	// TODO - do a join and make a curry with prometheus
}
