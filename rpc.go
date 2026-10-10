package pug

import (
	"context"

	"connectrpc.com/connect"

	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/events/v1/eventsv1connect"
	"github.com/pug-sh/sdk-go/gen/sdk/profiles/v1/sdkprofilesv1connect"
	"github.com/pug-sh/sdk-go/gen/shared/activity/v1/activityv1connect"
	"github.com/pug-sh/sdk-go/gen/shared/insights/v1/insightsv1connect"
	"github.com/pug-sh/sdk-go/gen/shared/profiles/v1/profilesv1connect"
)

type rpcClients struct {
	events      eventsv1connect.EventsServiceClient
	profilesSDK sdkprofilesv1connect.ProfilesSDKServiceClient
	profiles    profilesv1connect.ProfilesServiceClient
	activity    activityv1connect.ActivityServiceClient
	insights    insightsv1connect.InsightsServiceClient
}

func newRPCClients(httpClient connect.HTTPClient, endpoint, apiKey string) rpcClients {
	opt := connect.WithInterceptors(apiKeyInterceptor{key: apiKey})
	return rpcClients{
		events:      eventsv1connect.NewEventsServiceClient(httpClient, endpoint, opt),
		profilesSDK: sdkprofilesv1connect.NewProfilesSDKServiceClient(httpClient, endpoint, opt),
		profiles:    profilesv1connect.NewProfilesServiceClient(httpClient, endpoint, opt),
		activity:    activityv1connect.NewActivityServiceClient(httpClient, endpoint, opt),
		insights:    insightsv1connect.NewInsightsServiceClient(httpClient, endpoint, opt),
	}
}

type apiKeyInterceptor struct{ key string }

func (i apiKeyInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("x-api-key", i.key)
		return next(ctx, req)
	}
}

func (i apiKeyInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("x-api-key", i.key)
		return conn
	}
}

func (apiKeyInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

type eventSink struct {
	client eventsv1connect.EventsServiceClient
	log    logger
}

func (s eventSink) send(ctx context.Context, events []*eventsv1.Event) error {
	res, err := s.client.BatchCreate(ctx, connect.NewRequest(&eventsv1.BatchCreateRequest{Events: events}))
	if err != nil {
		return err
	}
	// A refused event isn't an RPC error, so this is the only sign it was lost.
	if dropped := res.Msg.GetDropped(); dropped > 0 {
		s.log.warn("server dropped events", "dropped", dropped, "sent", len(events), "reasons", res.Msg.GetDroppedByReason())
	}
	return nil
}
