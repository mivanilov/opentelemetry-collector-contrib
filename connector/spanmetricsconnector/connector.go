// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package spanmetricsconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector"

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lightstep/go-expohisto/structure"
	"github.com/tilinna/clock"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector/internal/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

const (
	serviceNameKey     = conventions.AttributeServiceName
	spanNameKey        = "span.name"   // OpenTelemetry non-standard constant.
	spanKindKey        = "span.kind"   // OpenTelemetry non-standard constant.
	statusCodeKey      = "status.code" // OpenTelemetry non-standard constant.
	metricKeySeparator = string(byte(0))

	defaultDimensionsCacheSize = 1000

	metricNameDuration = "duration"
	metricNameCalls    = "calls"

	defaultUnit = metrics.Milliseconds
	lineLength  = 120
)

type connectorImp struct {
	lock          sync.Mutex
	logger        *zap.Logger
	sugaredLogger *zap.SugaredLogger
	config        Config

	metricsConsumer consumer.Metrics

	// Additional dimensions to add to metrics.
	dimensions []dimension

	// The starting time of the data points.
	startTimestamp pcommon.Timestamp

	resourceMetrics map[resourceKey]*resourceMetrics

	keyBuf *bytes.Buffer

	// An LRU cache of dimension key-value maps keyed by a unique identifier formed by a concatenation of its values:
	// e.g. { "foo/barOK": { "serviceName": "foo", "span.name": "/bar", "status_code": "OK" }}
	metricKeyToDimensions *cache.Cache[metrics.Key, pcommon.Map]

	ticker  *clock.Ticker
	done    chan struct{}
	started bool

	shutdownOnce sync.Once
}

type resourceMetrics struct {
	histograms metrics.HistogramMetrics
	sums       metrics.SumMetrics
	attributes pcommon.Map
}

type dimension struct {
	name  string
	value *pcommon.Value
}

type serviceReqTraceSpanGroups struct {
	serverSpan *ptrace.Span
	otherSpans []ptrace.Span
}

type serviceReqTraceSpansGrouped struct {
	serverSpan      ptrace.Span
	otherSpanGroups [][]ptrace.Span
}

type serviceReqTraceServerSpanDetails struct {
	hasExternalSpansWithError bool
	internalDurationTotal     float64
}

type customDuration struct {
	startTimestamp pcommon.Timestamp
	endTimestamp   pcommon.Timestamp
}

func newDimensions(cfgDims []Dimension) []dimension {
	if len(cfgDims) == 0 {
		return nil
	}
	dims := make([]dimension, len(cfgDims))
	for i := range cfgDims {
		dims[i].name = cfgDims[i].Name
		if cfgDims[i].Default != nil {
			val := pcommon.NewValueStr(*cfgDims[i].Default)
			dims[i].value = &val
		}
	}
	return dims
}

func newConnector(logger *zap.Logger, config component.Config, ticker *clock.Ticker) (*connectorImp, error) {
	logger.Info("Building spanmetrics connector")
	cfg := config.(*Config)

	metricKeyToDimensionsCache, err := cache.NewCache[metrics.Key, pcommon.Map](cfg.DimensionsCacheSize)
	if err != nil {
		return nil, err
	}

	return &connectorImp{
		logger:                logger,
		sugaredLogger:         logger.Sugar(),
		config:                *cfg,
		startTimestamp:        pcommon.NewTimestampFromTime(time.Now()),
		resourceMetrics:       make(map[resourceKey]*resourceMetrics),
		dimensions:            newDimensions(cfg.Dimensions),
		keyBuf:                bytes.NewBuffer(make([]byte, 0, 1024)),
		metricKeyToDimensions: metricKeyToDimensionsCache,
		ticker:                ticker,
		done:                  make(chan struct{}),
	}, nil
}

func initHistogramMetrics(cfg Config) metrics.HistogramMetrics {
	if cfg.Histogram.Disable {
		return nil
	}
	if cfg.Histogram.Exponential != nil {
		maxSize := structure.DefaultMaxSize
		if cfg.Histogram.Exponential.MaxSize != 0 {
			maxSize = cfg.Histogram.Exponential.MaxSize
		}
		return metrics.NewExponentialHistogramMetrics(maxSize)
	}

	var bounds []float64
	if cfg.Histogram.Explicit != nil && cfg.Histogram.Explicit.Buckets != nil {
		bounds = durationsToUnits(cfg.Histogram.Explicit.Buckets, unitDivider(cfg.Histogram.Unit))
	} else {
		switch cfg.Histogram.Unit {
		case metrics.Milliseconds:
			bounds = defaultHistogramBucketsMs
		case metrics.Seconds:
			bounds = make([]float64, len(defaultHistogramBucketsMs))
			for i, v := range defaultHistogramBucketsMs {
				bounds[i] = v / float64(time.Second.Milliseconds())
			}
		}
	}

	return metrics.NewExplicitHistogramMetrics(bounds)
}

// unitDivider returns a unit divider to convert nanoseconds to milliseconds or seconds.
func unitDivider(u metrics.Unit) int64 {
	return map[metrics.Unit]int64{
		metrics.Seconds:      time.Second.Nanoseconds(),
		metrics.Milliseconds: time.Millisecond.Nanoseconds(),
	}[u]
}

func durationsToUnits(vs []time.Duration, unitDivider int64) []float64 {
	vsm := make([]float64, len(vs))
	for i, v := range vs {
		vsm[i] = float64(v.Nanoseconds()) / float64(unitDivider)
	}
	return vsm
}

// Start implements the component.Component interface.
func (p *connectorImp) Start(ctx context.Context, _ component.Host) error {
	p.logger.Info("Starting spanmetrics connector")

	p.started = true
	go func() {
		for {
			select {
			case <-p.done:
				return
			case <-p.ticker.C:
				p.exportMetrics(ctx)
			}
		}
	}()

	return nil
}

// Shutdown implements the component.Component interface.
func (p *connectorImp) Shutdown(context.Context) error {
	p.shutdownOnce.Do(func() {
		p.logger.Info("Shutting down spanmetrics connector")
		if p.started {
			p.logger.Info("Stopping ticker")
			p.ticker.Stop()
			p.done <- struct{}{}
			p.started = false
		}
	})
	return nil
}

// Capabilities implements the consumer interface.
func (p *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeTraces implements the consumer.Traces interface.
// It aggregates the trace data to generate metrics.
func (p *connectorImp) ConsumeTraces(_ context.Context, traces ptrace.Traces) error {
	p.lock.Lock()
	if p.config.ExternalStatsExclusion != nil {
		p.generateServiceServerMetricsExcludingExternalStats(traces)
	} else {
		p.aggregateMetrics(traces)
	}
	p.lock.Unlock()
	return nil
}

func (p *connectorImp) exportMetrics(ctx context.Context) {
	p.lock.Lock()

	m := p.buildMetrics()
	p.resetState()

	// This component no longer needs to read the metrics once built, so it is safe to unlock.
	p.lock.Unlock()

	if err := p.metricsConsumer.ConsumeMetrics(ctx, m); err != nil {
		p.logger.Error("Failed ConsumeMetrics", zap.Error(err))
		return
	}
}

// buildMetrics collects the computed raw metrics data and builds OTLP metrics.
func (p *connectorImp) buildMetrics() pmetric.Metrics {
	m := pmetric.NewMetrics()
	for _, rawMetrics := range p.resourceMetrics {
		rm := m.ResourceMetrics().AppendEmpty()
		rawMetrics.attributes.CopyTo(rm.Resource().Attributes())

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("spanmetricsconnector")

		sums := rawMetrics.sums
		metric := sm.Metrics().AppendEmpty()
		metric.SetName(buildMetricName(p.config.Namespace, metricNameCalls))
		sums.BuildMetrics(metric, p.startTimestamp, p.config.GetAggregationTemporality())
		if !p.config.Histogram.Disable {
			histograms := rawMetrics.histograms
			metric = sm.Metrics().AppendEmpty()
			metric.SetName(buildMetricName(p.config.Namespace, metricNameDuration))
			metric.SetUnit(p.config.Histogram.Unit.String())
			histograms.BuildMetrics(metric, p.startTimestamp, p.config.GetAggregationTemporality())
		}
	}

	return m
}

func (p *connectorImp) resetState() {
	// If delta metrics, reset accumulated data
	if p.config.GetAggregationTemporality() == pmetric.AggregationTemporalityDelta {
		p.resourceMetrics = make(map[resourceKey]*resourceMetrics)
		p.metricKeyToDimensions.Purge()
		p.startTimestamp = pcommon.NewTimestampFromTime(time.Now())
	} else {
		p.metricKeyToDimensions.RemoveEvictedItems()

		// Exemplars are only relevant to this batch of traces, so must be cleared within the lock
		if p.config.Histogram.Disable {
			return
		}
		for _, m := range p.resourceMetrics {
			m.histograms.Reset(true)
		}

	}
}

// aggregateMetrics aggregates the raw metrics from the input trace data.
//
// Metrics are grouped by resource attributes.
// Each metric is identified by a key that is built from the service name
// and span metadata such as name, kind, status_code and any additional
// dimensions the user has configured.
func (p *connectorImp) aggregateMetrics(traces ptrace.Traces) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		resourceAttr := rspans.Resource().Attributes()
		serviceAttr, ok := resourceAttr.Get(conventions.AttributeServiceName)
		if !ok {
			continue
		}

		rm := p.getOrCreateResourceMetrics(resourceAttr)
		sums := rm.sums
		histograms := rm.histograms

		unitDivider := unitDivider(p.config.Histogram.Unit)
		serviceName := serviceAttr.Str()
		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			ils := ilsSlice.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// Protect against end timestamps before start timestamps. Assume 0 duration.
				duration := float64(0)
				startTime := span.StartTimestamp()
				endTime := span.EndTimestamp()
				if endTime > startTime {
					duration = float64(endTime-startTime) / float64(unitDivider)
				}
				key := p.buildKey(serviceName, span, p.dimensions, resourceAttr)

				attributes, ok := p.metricKeyToDimensions.Get(key)
				if !ok {
					attributes = p.buildAttributes(serviceName, span, resourceAttr)
					p.metricKeyToDimensions.Add(key, attributes)
				}
				if !p.config.Histogram.Disable {
					// aggregate histogram metrics
					h := histograms.GetOrCreate(key, attributes)
					p.addExemplar(span, duration, h)
					h.Observe(duration)

				}
				// aggregate sums metrics
				s := sums.GetOrCreate(key, attributes)
				s.Add(1)
			}
		}
	}
}

func (p *connectorImp) generateServiceServerMetricsExcludingExternalStats(traces ptrace.Traces) {
	serverSpanDetailsByServiceReqTrace := p.getServerSpanDetailsByServiceReqTrace(traces)

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		resourceAttr := rspans.Resource().Attributes()
		serviceAttr, ok := resourceAttr.Get(conventions.AttributeServiceName)
		if !ok {
			continue
		}

		rm := p.getOrCreateResourceMetrics(resourceAttr)
		sums := rm.sums
		histograms := rm.histograms

		unitDivider := unitDivider(p.config.Histogram.Unit)
		serviceName := serviceAttr.Str()
		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			ils := ilsSlice.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				serviceReqTraceKey := getServiceReqTraceKey(serviceAttr, span.TraceID(), span.SpanID())
				if span.Kind() != ptrace.SpanKindServer {
					continue
				}
				duration := serverSpanDetailsByServiceReqTrace[serviceReqTraceKey].internalDurationTotal / float64(unitDivider)
				key := p.buildKey(serviceName, span, p.dimensions, resourceAttr)
				attributes, ok := p.metricKeyToDimensions.Get(key)
				if !ok {
					attributes = p.buildAttributes(serviceName, span, resourceAttr)
					p.metricKeyToDimensions.Add(key, attributes)
				}
				if serverSpanDetailsByServiceReqTrace[serviceReqTraceKey].hasExternalSpansWithError && p.config.ExternalStatsExclusion.StatusAttribute.isSpanStatusError(span) {
					attributes.Remove(p.config.ExternalStatsExclusion.StatusAttribute.AttributeName)
				}
				if !p.config.Histogram.Disable {
					// aggregate histogram metrics
					h := histograms.GetOrCreate(key, attributes)
					p.addExemplar(span, duration, h)
					h.Observe(duration)
				}
				// aggregate sums metrics
				s := sums.GetOrCreate(key, attributes)
				s.Add(1)
			}
		}
	}
}

func (p *connectorImp) getServerSpanDetailsByServiceReqTrace(traces ptrace.Traces) map[string]*serviceReqTraceServerSpanDetails {
	err := p.config.ExternalStatsExclusion.HostAttribute.compileRegex()
	if err != nil {
		p.logger.Error("unable to extract external traces", zap.Error(err))
		return make(map[string]*serviceReqTraceServerSpanDetails)
	}

	var serverSpanDetailsByServiceReqTrace = make(map[string]*serviceReqTraceServerSpanDetails)

	for serviceReqTraceKey, serviceReqTraceSpanGroups := range p.getGroupedSpansByServiceReqTrace(p.getUngroupedSpansByServiceReqTrace(traces)) {
		var internalDurationTotal float64
		var hasExternalSpansWithError bool

		hasExternalSpansWithError, internalDurationTotal = p.calcInternalServerSpanDetails(serviceReqTraceSpanGroups)

		serverSpanDetailsByServiceReqTrace[serviceReqTraceKey] = &serviceReqTraceServerSpanDetails{
			hasExternalSpansWithError: hasExternalSpansWithError,
			internalDurationTotal:     internalDurationTotal,
		}

		p.logDiagnostics(serviceReqTraceKey, serviceReqTraceSpanGroups, internalDurationTotal, hasExternalSpansWithError, traces)
	}

	return serverSpanDetailsByServiceReqTrace
}

func (p *connectorImp) calcInternalServerSpanDetails(serviceReqTraceSpanGroups *serviceReqTraceSpansGrouped) (bool, float64) {
	var externalSpansTotal []ptrace.Span
	var internalDuration = customDuration{
		startTimestamp: 0,
		endTimestamp:   0,
	}
	var hasExternalSpansWithError bool

	for _, spansGroup := range serviceReqTraceSpanGroups.otherSpanGroups {
		var externalSpans []ptrace.Span

		for _, span := range spansGroup {
			if p.config.ExternalStatsExclusion.HostAttribute.isSpanExternal(span) {
				if p.config.ExternalStatsExclusion.StatusAttribute.isSpanStatusError(span) {
					hasExternalSpansWithError = true
				}
				externalSpans = append(externalSpans, span)
				externalSpansTotal = append(externalSpansTotal, span)
			} else {
				internalDuration = p.calcInternalDuration(span, externalSpans, internalDuration)
			}
		}
	}

	var internalDurationTotal = float64(internalDuration.endTimestamp - internalDuration.startTimestamp)

	// in case the service trace consists only out of external client spans
	if internalDurationTotal == 0 && len(externalSpansTotal) != 0 {
		internalDurationTotal = p.calcInternalDurationHavingExternalSpansOnly(serviceReqTraceSpanGroups.serverSpan, externalSpansTotal)
	}

	serverSpanDurationTotal := float64(serviceReqTraceSpanGroups.serverSpan.EndTimestamp() - serviceReqTraceSpanGroups.serverSpan.StartTimestamp())
	if internalDurationTotal > serverSpanDurationTotal || internalDurationTotal == 0 {
		internalDurationTotal = serverSpanDurationTotal
	}

	return hasExternalSpansWithError, internalDurationTotal
}

func (p *connectorImp) getGroupedSpansByServiceReqTrace(ungroupedSpansByServiceReqTrace map[string]*serviceReqTraceSpanGroups) map[string]*serviceReqTraceSpansGrouped {
	var groupedSpansByServiceReqTrace = make(map[string]*serviceReqTraceSpansGrouped)

	for serviceReqTraceKey, ungroupedSpans := range ungroupedSpansByServiceReqTrace {
		sort.Slice(ungroupedSpans.otherSpans, func(i, j int) bool {
			return ungroupedSpans.otherSpans[i].StartTimestamp() < ungroupedSpans.otherSpans[j].StartTimestamp()
		})

		var groupedSpans [][]ptrace.Span
		for _, ungroupedSpan := range ungroupedSpans.otherSpans {
			spanTargetGroupIndex := -1
			if len(groupedSpans) != 0 {
				minStartEndDelta := int64(ungroupedSpan.StartTimestamp() - groupedSpans[0][len(groupedSpans[0])-1].EndTimestamp())
				for i, spansGroup := range groupedSpans {
					startEndDelta := int64(ungroupedSpan.StartTimestamp() - spansGroup[len(spansGroup)-1].EndTimestamp())
					if startEndDelta >= 0 && (startEndDelta <= minStartEndDelta || minStartEndDelta < 0) {
						minStartEndDelta = startEndDelta
						spanTargetGroupIndex = i
					}
				}

				if spanTargetGroupIndex != -1 {
					groupedSpans[spanTargetGroupIndex] = append(groupedSpans[spanTargetGroupIndex], ungroupedSpan)
				}

				if p.config.ExternalStatsExclusion.LogDebugInfo.Enabled {
					fmt.Printf("ServiceReqTraceKey=%s ungrouped spanId=%s minStartEndDelta=%d ns \n", serviceReqTraceKey, ungroupedSpan.SpanID(), minStartEndDelta)
				}
			}

			if spanTargetGroupIndex == -1 {
				groupedSpans = append(groupedSpans, []ptrace.Span{ungroupedSpan})
			}
		}

		groupedSpansByServiceReqTrace[serviceReqTraceKey] = &serviceReqTraceSpansGrouped{
			serverSpan:      *ungroupedSpans.serverSpan,
			otherSpanGroups: groupedSpans,
		}
	}

	return groupedSpansByServiceReqTrace
}

func (p *connectorImp) getUngroupedSpansByServiceReqTrace(traces ptrace.Traces) map[string]*serviceReqTraceSpanGroups {
	ungroupedSpansByServiceReqTrace := p.getServerSpansByServiceReqTrace(traces)
	p.addOtherSpansByServiceReqTrace(traces, ungroupedSpansByServiceReqTrace)
	return ungroupedSpansByServiceReqTrace
}

func (p *connectorImp) getServerSpansByServiceReqTrace(traces ptrace.Traces) map[string]*serviceReqTraceSpanGroups {
	var ungroupedServerSpansByServiceReqTrace = make(map[string]*serviceReqTraceSpanGroups)

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		serviceAttr, serviceAttrOk := rspans.Resource().Attributes().Get(conventions.AttributeServiceName)
		if !serviceAttrOk {
			p.logger.Warn("Service attribute missing!")
			continue
		}

		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			spans := ilsSlice.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.Kind() == ptrace.SpanKindServer {
					serviceReqTraceKey := getServiceReqTraceKey(serviceAttr, span.TraceID(), span.SpanID())
					if _, exists := ungroupedServerSpansByServiceReqTrace[serviceReqTraceKey]; !exists {
						ungroupedServerSpansByServiceReqTrace[serviceReqTraceKey] = &serviceReqTraceSpanGroups{
							serverSpan: &span,
							otherSpans: []ptrace.Span{},
						}
					}
				}
			}
		}
	}

	return ungroupedServerSpansByServiceReqTrace
}

func (p *connectorImp) addOtherSpansByServiceReqTrace(traces ptrace.Traces, ungroupedSpansByServiceReqTrace map[string]*serviceReqTraceSpanGroups) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		serviceAttr, serviceAttrOk := rspans.Resource().Attributes().Get(conventions.AttributeServiceName)
		if !serviceAttrOk {
			p.logger.Warn("Service attribute missing!")
			continue
		}

		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			spans := ilsSlice.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.Kind() == ptrace.SpanKindClient || span.Kind() == ptrace.SpanKindConsumer || span.Kind() == ptrace.SpanKindProducer {
					serviceReqTraceKey := getServiceReqTraceKey(serviceAttr, span.TraceID(), span.ParentSpanID())
					if _, exists := ungroupedSpansByServiceReqTrace[serviceReqTraceKey]; exists {
						ungroupedSpansByServiceReqTrace[serviceReqTraceKey].otherSpans = append(ungroupedSpansByServiceReqTrace[serviceReqTraceKey].otherSpans, span)
					}
				}
			}
		}
	}
}

func getServiceReqTraceKey(serviceName pcommon.Value, traceId pcommon.TraceID, serverSpanId pcommon.SpanID) string {
	return serviceName.Str() + "|" + traceId.String() + "|" + serverSpanId.String()
}

func splitServiceReqTraceKey(serviceReqTraceKey string) (string, string, string) {
	split := strings.Split(serviceReqTraceKey, "|")
	return split[0], split[1], split[2]
}

func (p *connectorImp) calcInternalDuration(span ptrace.Span, externalSpans []ptrace.Span, internalDuration customDuration) customDuration {
	spanStartTimestamp := span.StartTimestamp()
	spanEndTimestamp := span.EndTimestamp()

	if len(externalSpans) != 0 {
		for _, externalSpan := range externalSpans {
			externalSpanDuration := externalSpan.EndTimestamp() - externalSpan.StartTimestamp()
			spanStartTimestamp = spanStartTimestamp - externalSpanDuration
			spanEndTimestamp = spanEndTimestamp - externalSpanDuration
		}
	}

	if internalDuration.startTimestamp == 0 && internalDuration.endTimestamp == 0 {
		internalDuration.startTimestamp = spanStartTimestamp
		internalDuration.endTimestamp = spanEndTimestamp
	} else {
		if internalDuration.startTimestamp > spanStartTimestamp {
			internalDuration.startTimestamp = spanStartTimestamp
		}
		if internalDuration.endTimestamp < spanEndTimestamp {
			internalDuration.endTimestamp = spanEndTimestamp
		}
	}

	return internalDuration
}

func (p *connectorImp) calcInternalDurationHavingExternalSpansOnly(serverSpan ptrace.Span, externalSpansTotal []ptrace.Span) float64 {
	externalIoStartTimestamp := externalSpansTotal[0].StartTimestamp()
	externalIoEndTimestamp := externalSpansTotal[0].EndTimestamp()

	for _, externalSpan := range externalSpansTotal {
		if externalIoStartTimestamp > externalSpan.StartTimestamp() {
			externalIoStartTimestamp = externalSpan.StartTimestamp()
		}
		if externalIoEndTimestamp < externalSpan.EndTimestamp() {
			externalIoEndTimestamp = externalSpan.EndTimestamp()
		}
	}

	return float64(serverSpan.EndTimestamp()-serverSpan.StartTimestamp()) - float64(externalIoEndTimestamp-externalIoStartTimestamp)
}

func getSpanDuration(span ptrace.Span) float64 {
	return float64(span.EndTimestamp() - span.StartTimestamp())
}

func getSpanAttribute(span ptrace.Span, attributeName string) string {
	attr, found := span.Attributes().Get(attributeName)
	attrStr := ""
	if found {
		attrStr = attr.AsString()
	}
	return attrStr
}

func logSpansVisual(serviceSpanGroups *serviceReqTraceSpansGrouped) {
	dashes := ""
	serverSpanStr := ""
	serverAttributes := serviceSpanGroups.serverSpan.Attributes()
	serverSpanName, found := serverAttributes.Get("http.url")
	if found {
		serverSpanStr += strings.Split(serverSpanName.AsString(), "//")[1]
	}
	for i := 0; i < lineLength; i++ {
		if i < len(serverSpanStr) {
			dashes += string(serverSpanStr[i])
		} else {
			dashes += "-"
		}
	}
	fmt.Print("Service Trace spans visualization:\n")
	fmt.Println(dashes)

	var spansVisual string
	for _, spansGroup := range serviceSpanGroups.otherSpanGroups {
		serverDuration := getSpanDuration(serviceSpanGroups.serverSpan)
		curLine := ""
		for _, span := range spansGroup {
			spanDuration := getSpanDuration(span)
			spanStartFraction := float64(span.StartTimestamp()-serviceSpanGroups.serverSpan.StartTimestamp()) / serverDuration
			spanDurationFraction := spanDuration / serverDuration
			lineLocation := int(spanStartFraction * lineLength)
			numSpacesToAdd := lineLocation - len(curLine)
			for i := 0; i < numSpacesToAdd; i++ {
				curLine += " "
			}
			curLine += spanStr(span, spanDurationFraction)
		}
		spansVisual += curLine + "\n"
	}
	fmt.Print(spansVisual)
	fmt.Println(dashes)
}

func spanStr(span ptrace.Span, spanDurationFraction float64) string {
	output := ""
	var spanName string
	url, found := span.Attributes().Get("http.url")
	if found {
		spanName = strings.Split(url.AsString(), "//")[1]
	} else {
		spanName = span.Name()
	}
	for i := 0; i < int(spanDurationFraction*lineLength); i++ {
		if i < len(spanName) {
			output += string(spanName[i])
		} else {
			output += "="
		}
	}
	return output + " "
}

func (p *connectorImp) logSpanGroups(serviceSpanGroups *serviceReqTraceSpansGrouped) {
	unitDivider := unitDivider(p.config.Histogram.Unit)
	fmt.Print("Service request trace span groups: \n")
	fmt.Printf("  - Server span: SpanKind=%s, TraceID=%s, SpanID=%s, ParentSpanID=%s, Status=%s, Duration=%f %s, StartTime=%s, EndTime=%s, Attributes=%v \n",
		serviceSpanGroups.serverSpan.Kind(), serviceSpanGroups.serverSpan.TraceID(), serviceSpanGroups.serverSpan.SpanID(), serviceSpanGroups.serverSpan.ParentSpanID(), serviceSpanGroups.serverSpan.Status().Code().String(),
		float64(serviceSpanGroups.serverSpan.EndTimestamp()-serviceSpanGroups.serverSpan.StartTimestamp())/float64(unitDivider), p.config.Histogram.Unit.String(),
		serviceSpanGroups.serverSpan.StartTimestamp().String(), serviceSpanGroups.serverSpan.EndTimestamp().String(), serviceSpanGroups.serverSpan.Attributes().AsRaw())
	for i, otherSpansGroup := range serviceSpanGroups.otherSpanGroups {
		fmt.Printf("  - Other spans group %d: \n", i)
		for _, otherSpans := range otherSpansGroup {
			fmt.Printf("    - Other span: SpanKind=%s, TraceID=%s, SpanID=%s, ParentSpanID=%s, Status=%s, Duration=%f %s, StartTime=%s, EndTime=%s, Attributes=%v \n",
				otherSpans.Kind(), otherSpans.TraceID(), otherSpans.SpanID(), otherSpans.ParentSpanID(), otherSpans.Status().Code().String(),
				float64(otherSpans.EndTimestamp()-otherSpans.StartTimestamp())/float64(unitDivider), p.config.Histogram.Unit.String(),
				otherSpans.StartTimestamp().String(), otherSpans.EndTimestamp().String(), otherSpans.Attributes().AsRaw())
		}
	}
}

func (p *connectorImp) logServiceReqTraceSpans(serviceReqTraceKey string, traces ptrace.Traces) {
	serviceName, traceId, serverSpanId := splitServiceReqTraceKey(serviceReqTraceKey)
	var serviceReqTraceSpans []ptrace.Span

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rspans := traces.ResourceSpans().At(i)
		serviceAttr, serviceAttrOk := rspans.Resource().Attributes().Get(conventions.AttributeServiceName)
		if !serviceAttrOk || serviceAttr.Str() != serviceName {
			continue
		}

		ilsSlice := rspans.ScopeSpans()
		for j := 0; j < ilsSlice.Len(); j++ {
			spans := ilsSlice.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.TraceID().String() == traceId && (span.SpanID().String() == serverSpanId || span.ParentSpanID().String() == serverSpanId) {
					serviceReqTraceSpans = append(serviceReqTraceSpans, span)
				}
			}
		}
	}

	sort.Slice(serviceReqTraceSpans, func(i, j int) bool {
		return serviceReqTraceSpans[i].StartTimestamp() < serviceReqTraceSpans[j].StartTimestamp()
	})

	unitDivider := unitDivider(p.config.Histogram.Unit)
	fmt.Print("Service request trace all spans: \n")
	for _, span := range serviceReqTraceSpans {
		fmt.Printf("  - Span: SpanKind=%s, TraceID=%s, SpanID=%s, ParentSpanID=%s, Status=%s, Duration=%f %s, StartTime=%s, EndTime=%s, Attributes=%v \n",
			span.Kind(), span.TraceID(), span.SpanID(), span.ParentSpanID(), span.Status().Code().String(),
			float64(span.EndTimestamp()-span.StartTimestamp())/float64(unitDivider), p.config.Histogram.Unit.String(),
			span.StartTimestamp().String(), span.EndTimestamp().String(), span.Attributes().AsRaw())
	}
}

func (p *connectorImp) logDiagnostics(serviceReqTraceKey string, serviceReqTraceSpanGroups *serviceReqTraceSpansGrouped, internalDurationTotal float64, hasExternalSpansWithError bool, traces ptrace.Traces) {
	serverSpanDurationTotal := float64(serviceReqTraceSpanGroups.serverSpan.EndTimestamp() - serviceReqTraceSpanGroups.serverSpan.StartTimestamp())
	if internalDurationTotal == 0 {
		internalDurationTotal = serverSpanDurationTotal
	}

	if p.config.ExternalStatsExclusion.LogDebugInfo.Enabled && !p.skipLogDebugInfo(serviceReqTraceSpanGroups.serverSpan) {
		url := getSpanAttribute(serviceReqTraceSpanGroups.serverSpan, "http.url")
		unitDivider := unitDivider(p.config.Histogram.Unit)
		fmt.Println("\n!!!!!!! Start Debug Info !!!!!!!")
		fmt.Printf("ServiceReqTraceKey=%s, httpRoute=%s \n"+
			"Service request server span details: \n"+
			"  - Total duration: %f %s \n"+
			"  - Internal duration: %f %s \n"+
			"  - Has external spans with error: %t \n",
			serviceReqTraceKey, url, float64(serviceReqTraceSpanGroups.serverSpan.EndTimestamp()-serviceReqTraceSpanGroups.serverSpan.StartTimestamp())/float64(unitDivider), p.config.Histogram.Unit.String(),
			internalDurationTotal/float64(unitDivider), p.config.Histogram.Unit.String(), hasExternalSpansWithError)
		//logSpansVisual(serviceReqTraceSpanGroups) FIXME - malfunctioning, causing OOM errors
		p.logSpanGroups(serviceReqTraceSpanGroups)
		p.logServiceReqTraceSpans(serviceReqTraceKey, traces)
		fmt.Println("!!!!!!! End Debug Info !!!!!!!")
		fmt.Println()
	}

	if internalDurationTotal > serverSpanDurationTotal {
		url := getSpanAttribute(serviceReqTraceSpanGroups.serverSpan, "http.url")
		unitDivider := unitDivider(p.config.Histogram.Unit)
		fmt.Println("\n!!!!!!! Begin: calculated internal duration > server span duration !!!!!!!")
		fmt.Printf("ServiceReqTraceKey=%s httpRoute=%s otherSpanGroupsCount=%d calculated total_internal_duration=%f%s is more than server_span_duration=%f%s!",
			serviceReqTraceKey, url, len(serviceReqTraceSpanGroups.otherSpanGroups),
			internalDurationTotal/float64(unitDivider), p.config.Histogram.Unit.String(),
			serverSpanDurationTotal/float64(unitDivider), p.config.Histogram.Unit.String())
		if !p.config.ExternalStatsExclusion.LogDebugInfo.Enabled {
			p.logSpanGroups(serviceReqTraceSpanGroups)
		}
		fmt.Println("!!!!!!! End: calculated internal duration > server span duration !!!!!!!")
		fmt.Println()
	}
}

func (p *connectorImp) skipLogDebugInfo(span ptrace.Span) bool {
	return contains(p.config.ExternalStatsExclusion.LogDebugInfo.RoutesToIgnore, getSpanAttribute(span, "http.route")) ||
		contains(p.config.ExternalStatsExclusion.LogDebugInfo.RoutesToIgnore, getSpanAttribute(span, "http.target"))
}

func (p *connectorImp) addExemplar(span ptrace.Span, duration float64, h metrics.Histogram) {
	if !p.config.Exemplars.Enabled {
		return
	}
	if span.TraceID().IsEmpty() {
		return
	}

	h.AddExemplar(span.TraceID(), span.SpanID(), duration)
}

type resourceKey [16]byte

func (p *connectorImp) getOrCreateResourceMetrics(attr pcommon.Map) *resourceMetrics {
	key := resourceKey(pdatautil.MapHash(attr))
	v, ok := p.resourceMetrics[key]
	if !ok {
		v = &resourceMetrics{
			histograms: initHistogramMetrics(p.config),
			sums:       metrics.NewSumMetrics(),
			attributes: attr,
		}
		p.resourceMetrics[key] = v
	}
	return v
}

// contains checks if string slice contains a string value
func contains(elements []string, value string) bool {
	for _, element := range elements {
		if value == element {
			return true
		}
	}
	return false
}

func (p *connectorImp) buildAttributes(serviceName string, span ptrace.Span, resourceAttrs pcommon.Map) pcommon.Map {
	attr := pcommon.NewMap()
	attr.EnsureCapacity(4 + len(p.dimensions))
	if !contains(p.config.ExcludeDimensions, serviceNameKey) {
		attr.PutStr(serviceNameKey, serviceName)
	}
	if !contains(p.config.ExcludeDimensions, spanNameKey) {
		attr.PutStr(spanNameKey, span.Name())
	}
	if !contains(p.config.ExcludeDimensions, spanKindKey) {
		attr.PutStr(spanKindKey, traceutil.SpanKindStr(span.Kind()))
	}
	if !contains(p.config.ExcludeDimensions, statusCodeKey) {
		attr.PutStr(statusCodeKey, traceutil.StatusCodeStr(span.Status().Code()))
	}
	for _, d := range p.dimensions {
		if v, ok := getDimensionValue(d, span.Attributes(), resourceAttrs); ok {
			v.CopyTo(attr.PutEmpty(d.name))
		}
	}
	return attr
}

func concatDimensionValue(dest *bytes.Buffer, value string, prefixSep bool) {
	if prefixSep {
		dest.WriteString(metricKeySeparator)
	}
	dest.WriteString(value)
}

// buildKey builds the metric key from the service name and span metadata such as name, kind, status_code and
// will attempt to add any additional dimensions the user has configured that match the span's attributes
// or resource attributes. If the dimension exists in both, the span's attributes, being the most specific, takes precedence.
//
// The metric key is a simple concatenation of dimension values, delimited by a null character.
func (p *connectorImp) buildKey(serviceName string, span ptrace.Span, optionalDims []dimension, resourceAttrs pcommon.Map) metrics.Key {
	p.keyBuf.Reset()
	if !contains(p.config.ExcludeDimensions, serviceNameKey) {
		concatDimensionValue(p.keyBuf, serviceName, false)
	}
	if !contains(p.config.ExcludeDimensions, spanNameKey) {
		concatDimensionValue(p.keyBuf, span.Name(), true)
	}
	if !contains(p.config.ExcludeDimensions, spanKindKey) {
		concatDimensionValue(p.keyBuf, traceutil.SpanKindStr(span.Kind()), true)
	}
	if !contains(p.config.ExcludeDimensions, statusCodeKey) {
		concatDimensionValue(p.keyBuf, traceutil.StatusCodeStr(span.Status().Code()), true)
	}

	for _, d := range optionalDims {
		if v, ok := getDimensionValue(d, span.Attributes(), resourceAttrs); ok {
			concatDimensionValue(p.keyBuf, v.AsString(), true)
		}
	}

	return metrics.Key(p.keyBuf.String())
}

// getDimensionValue gets the dimension value for the given configured dimension.
// It searches through the span's attributes first, being the more specific;
// falling back to searching in resource attributes if it can't be found in the span.
// Finally, falls back to the configured default value if provided.
//
// The ok flag indicates if a dimension value was fetched in order to differentiate
// an empty string value from a state where no value was found.
func getDimensionValue(d dimension, spanAttr pcommon.Map, resourceAttr pcommon.Map) (v pcommon.Value, ok bool) {
	// The more specific span attribute should take precedence.
	if attr, exists := spanAttr.Get(d.name); exists {
		return attr, true
	}
	if attr, exists := resourceAttr.Get(d.name); exists {
		return attr, true
	}
	// Set the default if configured, otherwise this metric will have no value set for the dimension.
	if d.value != nil {
		return *d.value, true
	}
	return v, ok
}

// buildMetricName builds the namespace prefix for the metric name.
func buildMetricName(namespace string, name string) string {
	if namespace != "" {
		return namespace + "." + name
	}
	return name
}
