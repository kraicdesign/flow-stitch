// Package projection defines the deterministic OpenSearch document projection.
package projection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/kraicdesign/flow-stitch/internal/domain/event"
	"github.com/kraicdesign/flow-stitch/internal/domain/flow"
	"github.com/kraicdesign/flow-stitch/internal/domain/rule"
)

// OutputID is the deterministic OpenSearch identity of one finalized flow batch.
type OutputID string

// Document is the stable OpenSearch representation of a finalized flow.
type Document struct {
	Timestamp time.Time      `json:"@timestamp"`
	Flow      Meta           `json:"flow"`
	Fields    map[string]any `json:"fields,omitempty"`
	Events    []View         `json:"events"`
	Anomalies []flow.Anomaly `json:"anomalies"`
}

// TypeCount records duplicate frequency for one producer event type.
type TypeCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// Meta contains flow-wide fields used for filtering and aggregation.
type Meta struct {
	ID                    string       `json:"id"`
	RuleID                rule.ID      `json:"rule_id"`
	RuleVersion           rule.Version `json:"rule_version"`
	Reason                flow.Reason  `json:"finalization_reason"`
	StartedAt             time.Time    `json:"started_at"`
	EndedAt               time.Time    `json:"ended_at"`
	DurationMS            int64        `json:"duration_ms"`
	AgeMS                 int64        `json:"age_ms"`
	EventCount            int          `json:"event_count"`
	EntryCount            int          `json:"entry_count"`
	DuplicateCount        int          `json:"duplicate_count"`
	IncompleteInvocations int          `json:"incomplete_invocations"`
	Duplicates            []TypeCount  `json:"duplicates"`
}

// View is map-shaped because configured role names become keys directly on a
// stitched entry (ADR-0005, section 4).
type View map[string]any

// NewOutputID separates repeated batches using their first observed time (ADR-0003, section 3).
func NewOutputID(ruleID rule.ID, correlationKey string, firstObservedAt time.Time) OutputID {
	source := string(ruleID) + ":" + correlationKey + ":" + firstObservedAt.Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(source))
	return OutputID(hex.EncodeToString(sum[:]))
}

// Project translates an immutable aggregate snapshot into the ADR-0005 output shape.
func Project(finalized flow.Finalized, configured rule.Rule) (Document, error) {
	startedAt, endedAt := flowTimes(finalized.Events, configured)
	document := Document{
		Timestamp: outputTimestamp(finalized, configured),
		Flow: Meta{
			ID: finalized.Key.CorrelationKey, RuleID: finalized.RuleID, RuleVersion: finalized.RuleVersion,
			Reason:    finalized.Reason,
			StartedAt: startedAt, EndedAt: endedAt, DurationMS: endedAt.Sub(startedAt).Milliseconds(),
			AgeMS:      finalized.FinalizedAt.Sub(finalized.FirstObservedAt).Milliseconds(),
			EventCount: len(finalized.Events), EntryCount: len(finalized.Entries),
			DuplicateCount: finalized.DuplicateCount, IncompleteInvocations: finalized.IncompleteInvocations,
		},
		Events:    make([]View, 0, len(finalized.Entries)),
		Anomalies: append([]flow.Anomaly(nil), finalized.Anomalies...),
	}

	duplicateTypes := make([]string, 0, len(finalized.Duplicates))
	for eventType := range finalized.Duplicates {
		duplicateTypes = append(duplicateTypes, eventType)
	}
	sort.Strings(duplicateTypes)
	for _, eventType := range duplicateTypes {
		document.Flow.Duplicates = append(document.Flow.Duplicates, TypeCount{Type: eventType, Count: finalized.Duplicates[eventType]})
	}
	if document.Flow.Duplicates == nil {
		document.Flow.Duplicates = []TypeCount{}
	}

	for _, entry := range finalized.Entries {
		if entry.Invocation != nil {
			view, clockSkew := invocationView(*entry.Invocation, configured)
			document.Events = append(document.Events, view)
			if clockSkew {
				document.Anomalies = append(document.Anomalies, flow.Anomaly{Kind: flow.AnomalyClockSkew, Detail: "invocation duration is negative", At: finalized.FinalizedAt})
			}
			continue
		}
		if entry.Plain != nil {
			document.Events = append(document.Events, plainView(*entry.Plain, configured))
		}
	}
	if document.Anomalies == nil {
		document.Anomalies = []flow.Anomaly{}
	}
	projectPromotions(&document, finalized, configured)
	return document, nil
}

func projectPromotions(document *Document, finalized flow.Finalized, configured rule.Rule) {
	promotions := append([]rule.Promotion(nil), configured.Promote...)
	sort.Slice(promotions, func(i, j int) bool { return promotions[i].Name < promotions[j].Name })
	for _, promoted := range promotions {
		raw, ok := promotionValue(finalized, promoted)
		if !ok {
			continue
		}
		value, err := coerce(raw, promoted.Type)
		if err != nil {
			document.Anomalies = append(document.Anomalies, flow.Anomaly{
				Kind:   flow.AnomalyPromotion,
				Detail: fmt.Sprintf("promoted field %q received %q, which is not %s", promoted.Name, raw, promoted.Type),
				At:     finalized.FinalizedAt,
			})
			continue
		}
		if document.Fields == nil {
			document.Fields = make(map[string]any)
		}
		document.Fields[promoted.Name] = value
	}
}

func promotionValue(finalized flow.Finalized, promoted rule.Promotion) (string, bool) {
	if len(finalized.Events) == 0 {
		return "", false
	}
	switch promoted.From {
	case "first":
		return promoted.Path.Resolve(finalized.Events[0].Doc)
	case "last":
		return promoted.Path.Resolve(finalized.Events[len(finalized.Events)-1].Doc)
	case "":
		for _, item := range finalized.Events {
			if value, ok := promoted.Path.Resolve(item.Doc); ok {
				return value, true
			}
		}
		return "", false
	default:
		for _, item := range finalized.Events {
			if !eventOccupiesRole(item, finalized.Invocations, promoted.From) {
				continue
			}
			return promoted.Path.Resolve(item.Doc)
		}
		return "", false
	}
}

func eventOccupiesRole(item event.Event, invocations []flow.Invocation, roleName string) bool {
	for _, invocation := range invocations {
		member, ok := invocation.Members[roleName]
		if ok && member.ObservedAt.Equal(item.ObservedAt) && reflect.DeepEqual(member.Doc, item.Doc) {
			return true
		}
	}
	return false
}

func coerce(raw string, target rule.PromotionType) (any, error) {
	switch target {
	case rule.PromotionKeyword:
		return raw, nil
	case rule.PromotionLong:
		return strconv.ParseInt(raw, 10, 64)
	case rule.PromotionDouble:
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("invalid double")
		}
		return value, nil
	case rule.PromotionBoolean:
		return strconv.ParseBool(raw)
	case rule.PromotionDate:
		value, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown promotion type %q", target)
	}
}

func invocationView(invocation flow.Invocation, configured rule.Rule) (View, bool) {
	stitch := stitchByID(configured, invocation.StitchID)
	startedAt, endedAt := invocationTimes(invocation, stitch, configured)
	duration := int64(-1)
	view := View{
		"group": invocation.Group, "complete": invocation.Complete,
		"started_at": startedAt, "duration_ms": duration,
	}
	if invocation.Complete {
		duration = endedAt.Sub(startedAt).Milliseconds()
		view["ended_at"] = endedAt
		view["duration_ms"] = duration
	}
	view["duplicate_count"] = invocation.DuplicateCount
	for _, role := range stitch.Roles {
		member, ok := invocation.Members[role.Name]
		if !ok {
			continue
		}
		eventType, _ := configured.Extract.EventType.Resolve(member.Doc)
		view[role.Name] = View{"type": eventType, "timestamp": eventTimestamp(member, configured), "event": member.Doc}
	}
	return view, duration < 0
}

func plainView(e event.Event, configured rule.Rule) View {
	eventType, _ := configured.Extract.EventType.Resolve(e.Doc)
	return View{"type": eventType, "timestamp": eventTimestamp(e, configured), "event": e.Doc}
}

func outputTimestamp(finalized flow.Finalized, configured rule.Rule) time.Time {
	if configured.Output.TimestampSource == rule.TimestampFinalized {
		return finalized.FinalizedAt
	}
	if len(finalized.Events) == 0 {
		return time.Time{}
	}
	first, last := flowTimes(finalized.Events, configured)
	if configured.Output.TimestampSource == rule.TimestampLastEvent {
		return last
	}
	return first
}

func flowTimes(events []event.Event, configured rule.Rule) (time.Time, time.Time) {
	if len(events) == 0 {
		return time.Time{}, time.Time{}
	}
	var first, last time.Time
	for _, item := range events {
		stamp := eventTimestamp(item, configured)
		if stamp.IsZero() {
			continue
		}
		if first.IsZero() || stamp.Before(first) {
			first = stamp
		}
		if last.IsZero() || stamp.After(last) {
			last = stamp
		}
	}
	return first, last
}

func invocationTimes(invocation flow.Invocation, stitch rule.Stitch, configured rule.Rule) (time.Time, time.Time) {
	var first, last time.Time
	required := stitch.Requires
	if len(required) == 0 {
		for _, role := range stitch.Roles {
			required = append(required, role.Name)
		}
	}
	for _, roleName := range required {
		member, ok := invocation.Members[roleName]
		if !ok {
			continue
		}
		stamp := eventTimestamp(member, configured)
		if first.IsZero() {
			first = stamp
		}
		last = stamp
	}
	if first.IsZero() {
		for _, role := range stitch.Roles {
			member, ok := invocation.Members[role.Name]
			if !ok {
				continue
			}
			stamp := eventTimestamp(member, configured)
			if first.IsZero() {
				first = stamp
			}
			last = stamp
		}
	}
	return first, last
}

func eventTimestamp(e event.Event, configured rule.Rule) time.Time {
	raw, ok := configured.Extract.Timestamp.Resolve(e.Doc)
	if !ok {
		return time.Time{}
	}
	stamp, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return stamp
}

func stitchByID(configured rule.Rule, id string) rule.Stitch {
	for _, stitch := range configured.Stitch {
		if stitch.ID == id {
			return stitch
		}
	}
	return rule.Stitch{}
}
