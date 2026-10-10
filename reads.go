package pug

import (
	"context"
	"iter"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
	activityv1 "github.com/pug-sh/sdk-go/gen/shared/activity/v1"
	insightsv1 "github.com/pug-sh/sdk-go/gen/shared/insights/v1"
	profilesv1 "github.com/pug-sh/sdk-go/gen/shared/profiles/v1"
)

// ProfilesClient reads and deletes profiles. Reads leave out traffic tagged as a bot at
// ingest unless asked to include it.
type ProfilesClient struct{ c *Client }

// Get returns the profile with the given Pug profile id.
func (p *ProfilesClient) Get(ctx context.Context, id string, opts ...GetOption) (*profilesv1.Profile, error) {
	o := getOpts(opts)
	res, err := unary(ctx, p.c, &profilesv1.GetRequest{Id: proto.String(id), IncludeBots: o.includeBots}, p.c.rpc.profiles.Get)
	return res.GetProfile(), err
}

// GetByExternalID returns the profile with the external id your application assigned it.
func (p *ProfilesClient) GetByExternalID(ctx context.Context, externalID string, opts ...GetOption) (*profilesv1.Profile, error) {
	o := getOpts(opts)
	req := &profilesv1.GetByExternalIdRequest{ExternalId: proto.String(externalID), IncludeBots: o.includeBots}
	res, err := unary(ctx, p.c, req, p.c.rpc.profiles.GetByExternalId)
	return res.GetProfile(), err
}

// Delete erases the profile with the given id. Erasure completes asynchronously; the response
// carries the request id to track it by.
func (p *ProfilesClient) Delete(ctx context.Context, id string) (*profilesv1.DeleteResponse, error) {
	return unary(ctx, p.c, &profilesv1.DeleteRequest{Id: proto.String(id)}, p.c.rpc.profiles.Delete)
}

// List iterates over the profiles matching req, fetching pages as it goes; a nil req lists
// every profile. An error ends the iteration. The whole iteration runs under one request, so
// without a deadline on ctx it is bounded by [Options.Timeout].
func (p *ProfilesClient) List(ctx context.Context, req *profilesv1.ListRequest) iter.Seq2[*profilesv1.Profile, error] {
	return func(yield func(*profilesv1.Profile, error) bool) {
		ctx, cancel := p.c.withTimeout(ctx)
		defer cancel()
		stream, err := p.c.rpc.profiles.List(ctx, connect.NewRequest(orEmpty(req)))
		if err != nil {
			yield(nil, toError(err))
			return
		}
		defer stream.Close()
		for stream.Receive() {
			for _, profile := range stream.Msg().GetProfiles() {
				if !yield(profile, nil) {
					return
				}
			}
		}
		if err := stream.Err(); err != nil {
			yield(nil, toError(err))
		}
	}
}

// ActivityClient reads raw events and per-profile activity. Reads leave out traffic tagged as
// a bot at ingest unless the request sets include_bots.
type ActivityClient struct{ c *Client }

// Feed returns a page of one profile's events, merged anonymous events included.
func (a *ActivityClient) Feed(ctx context.Context, req *activityv1.GetActivityFeedRequest) (*activityv1.GetActivityFeedResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetActivityFeed)
}

// EventExplorer returns a page of events across every profile in the project.
func (a *ActivityClient) EventExplorer(ctx context.Context, req *activityv1.GetEventExplorerRequest) (*activityv1.GetEventExplorerResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetEventExplorer)
}

// Heatmap returns one profile's per-day event counts.
func (a *ActivityClient) Heatmap(ctx context.Context, req *activityv1.GetActivityHeatmapRequest) (*activityv1.GetActivityHeatmapResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetActivityHeatmap)
}

// ProfileSessions returns a page of one profile's sessions.
func (a *ActivityClient) ProfileSessions(ctx context.Context, req *activityv1.GetProfileSessionsRequest) (*activityv1.GetProfileSessionsResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetProfileSessions)
}

// ProfileStats returns one profile's aggregate statistics and latest device and location
// context.
func (a *ActivityClient) ProfileStats(ctx context.Context, req *activityv1.GetProfileStatsRequest) (*activityv1.GetProfileStatsResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetProfileStats)
}

// FilterSchema returns the event kinds and property keys available to filter by; req may be
// nil.
func (a *ActivityClient) FilterSchema(ctx context.Context, req *commonv1.GetFilterSchemaRequest) (*commonv1.GetFilterSchemaResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetFilterSchema)
}

// PropertyValues returns the distinct values seen for a property key.
func (a *ActivityClient) PropertyValues(ctx context.Context, req *activityv1.GetPropertyValuesRequest) (*activityv1.GetPropertyValuesResponse, error) {
	return unary(ctx, a.c, req, a.c.rpc.activity.GetPropertyValues)
}

// InsightsClient runs product-analytics queries. Queries leave out traffic tagged as a bot at
// ingest unless the spec sets include_bots.
type InsightsClient struct{ c *Client }

// Query runs an insight (trends, segmentation, funnel, retention, user flow, top-K or map).
func (i *InsightsClient) Query(ctx context.Context, req *insightsv1.QueryRequest) (*insightsv1.QueryResponse, error) {
	return unary(ctx, i.c, req, i.c.rpc.insights.Query)
}

// SegmentUsers returns a page of the distinct ids behind a set of events.
func (i *InsightsClient) SegmentUsers(ctx context.Context, req *insightsv1.SegmentUsersRequest) (*insightsv1.SegmentUsersResponse, error) {
	return unary(ctx, i.c, req, i.c.rpc.insights.SegmentUsers)
}

// FilterSchema returns the event kinds and property keys available to filter and break down
// by; req may be nil.
func (i *InsightsClient) FilterSchema(ctx context.Context, req *commonv1.GetFilterSchemaRequest) (*commonv1.GetFilterSchemaResponse, error) {
	return unary(ctx, i.c, req, i.c.rpc.insights.GetFilterSchema)
}

// PropertyValues returns the observed values of an event or profile property.
func (i *InsightsClient) PropertyValues(ctx context.Context, req *insightsv1.GetPropertyValuesRequest) (*insightsv1.GetPropertyValuesResponse, error) {
	return unary(ctx, i.c, req, i.c.rpc.insights.GetPropertyValues)
}

func unary[Req, Res any](ctx context.Context, c *Client, req *Req, call func(context.Context, *connect.Request[Req]) (*connect.Response[Res], error)) (*Res, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	res, err := call(ctx, connect.NewRequest(orEmpty(req)))
	if err != nil {
		return nil, toError(err)
	}
	return res.Msg, nil
}

func orEmpty[T any](req *T) *T {
	if req == nil {
		return new(T)
	}
	return req
}

func getOpts(opts []GetOption) getOptions {
	var o getOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}
