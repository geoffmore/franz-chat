package lib

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

// See https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Registry

// NOTE - each of Gauge, Counter, Histogram, Summary are all collectors, so they can be registered/unregistered programmatically already

// franzDBStatsCollector is a prometheus.Collector
type franzDBStatsCollector struct {
	conn          *pgxpool.Conn
	ctx           context.Context
	usersTotal    *prometheus.Desc
	channelsTotal *prometheus.Desc
	messagesTotal *prometheus.Desc
	// inspired by https://github.com/prometheus/client_golang/blob/v1.20.5/prometheus/collectors/dbstats_collector.go#L40
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
	var (
		i        int
		channels = make(map[string]float64)
	)
	row := conn.QueryRow(ctx, "SELECT count(*) FROM messages;")
	if err := row.Scan(&i); HandlePGError(err, nil) != nil {
		log.Err(err)
		return channels
	}

	// TODO - use channel uuids instead once the channels feature is added
	channels["default"] = float64(i)
	return channels

	// TODO - do a join and make a curry with prometheus
	/*
		SELECT uuid from channels
		INNER JOIN messages ON channels.uuid = messages.channel_uuid;
	*/
	// The above query doesn't work because it would omit channels with no messages, which may actually be desirable.
}

/*
HandleSessionMetrics attempts to get a session lock and serves metrics if the lock is held.
By using a session lock, only one instance of an application will do work - regardless of replicas.
In this case, Prometheus metrics are served only on the lock holder.
Ideally, this would be a metric on franzDBStatsCollector to simplify the function signature, but then I would need to export that struct
*/
func HandleSessionMetrics(ctx context.Context, conn *pgxpool.Conn, logger *Logger, registry *prometheus.Registry, collector prometheus.Collector) {
	if getLock(ctx, conn) {
		logger.Debug("PostgreSQL session lock attempted and acquired! Serving additional metrics.")
		// TODO - register only once to avoid duplicate registration error
		// Check when trying to register. Ignore AlreadyRegisteredError since registration is being attempted explicitly
		if err := registry.Register(collector); err != nil && !errors.Is(err, prometheus.AlreadyRegisteredError{
			ExistingCollector: collector,
			NewCollector:      collector,
		}) {
			logger.Error("", err)
		}
	} else {
		logger.Debug("PostgreSQL session lock attempted and not acquired. No additional metrics served.")

		/* Attempt to unregister collector. In cases where this app starts without a session lock and later gains the lock.
		Should be a no-op if the collector is not registered
		*/
		_ = registry.Unregister(collector)
	}
	/* Backoff for lock reconciliation is the responsibility of the calling function. Ideally matches scrape_interval in prometheus
	It is important to note that the longer the reconciliation period, the more likely and the longer there will be a gap in metrics
	*/
}

// TODO - create pgxpoolStatsCollector or use database/sql with pgx driver
