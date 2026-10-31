package pug

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/pug-sh/sdk-go/gen/common/v1"
	eventsv1 "github.com/pug-sh/sdk-go/gen/sdk/events/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/events/v1/eventsv1connect"
	sdkprofilesv1 "github.com/pug-sh/sdk-go/gen/sdk/profiles/v1"
	"github.com/pug-sh/sdk-go/gen/sdk/profiles/v1/sdkprofilesv1connect"
	activityv1 "github.com/pug-sh/sdk-go/gen/shared/activity/v1"
	"github.com/pug-sh/sdk-go/gen/shared/activity/v1/activityv1connect"
	insightsv1 "github.com/pug-sh/sdk-go/gen/shared/insights/v1"
	"github.com/pug-sh/sdk-go/gen/shared/insights/v1/insightsv1connect"
	profilesv1 "github.com/pug-sh/sdk-go/gen/shared/profiles/v1"
	"github.com/pug-sh/sdk-go/gen/shared/profiles/v1/profilesv1connect"
)

// pugServer is an in-process stand-in for the Pug API, so these tests cross the real wire:
// HTTP, the Connect protocol, binary protobuf and the API key header.
type pugServer struct {
	eventsv1connect.UnimplementedEventsServiceHandler
	sdkprofilesv1connect.UnimplementedProfilesSDKServiceHandler
	profilesv1connect.UnimplementedProfilesServiceHandler

	mu         sync.Mutex
	headers    []http.Header
	batches    [][]*eventsv1.Event
	identifies []*sdkprofilesv1.IdentifyRequest
	batchErrs  []error
	listPages  [][]*profilesv1.Profile
	listErr    error
	statsDelay time.Duration
	lastGet    *profilesv1.GetByExternalIdRequest
	// legacy serves only Identify, as a server from before BatchIdentify does, so a
	// BatchIdentify call gets a real 404.
	legacy bool
}

func (s *pugServer) saw(h http.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headers = append(s.headers, h.Clone())
}

func (s *pugServer) BatchCreate(_ context.Context, req *connect.Request[eventsv1.BatchCreateRequest]) (*connect.Response[eventsv1.BatchCreateResponse], error) {
	s.saw(req.Header())
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.batchErrs) > 0 {
		err := s.batchErrs[0]
		s.batchErrs = s.batchErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	s.batches = append(s.batches, req.Msg.GetEvents())
	return connect.NewResponse(&eventsv1.BatchCreateResponse{Accepted: proto.Uint32(uint32(len(req.Msg.GetEvents())))}), nil
}

func (s *pugServer) Identify(_ context.Context, req *connect.Request[sdkprofilesv1.IdentifyRequest]) (*connect.Response[sdkprofilesv1.IdentifyResponse], error) {
	s.saw(req.Header())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.identifies = append(s.identifies, req.Msg)
	return connect.NewResponse(&sdkprofilesv1.IdentifyResponse{}), nil
}

func (s *pugServer) BatchIdentify(_ context.Context, req *connect.Request[sdkprofilesv1.BatchIdentifyRequest]) (*connect.Response[sdkprofilesv1.BatchIdentifyResponse], error) {
	s.saw(req.Header())
	s.mu.Lock()
	defer s.mu.Unlock()
	items := req.Msg.GetIdentifies()
	s.identifies = append(s.identifies, items...)
	return connect.NewResponse(&sdkprofilesv1.BatchIdentifyResponse{Accepted: proto.Uint32(uint32(len(items)))}), nil
}

func (s *pugServer) Get(_ context.Context, req *connect.Request[profilesv1.GetRequest]) (*connect.Response[profilesv1.GetResponse], error) {
	if req.Msg.GetId() != "p1" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("profile not found"))
	}
	return connect.NewResponse(&profilesv1.GetResponse{Profile: &profilesv1.Profile{Id: proto.String("p1")}}), nil
}

func (s *pugServer) GetByExternalId(_ context.Context, req *connect.Request[profilesv1.GetByExternalIdRequest]) (*connect.Response[profilesv1.GetByExternalIdResponse], error) {
	s.mu.Lock()
	s.lastGet = req.Msg
	s.mu.Unlock()
	return connect.NewResponse(&profilesv1.GetByExternalIdResponse{Profile: &profilesv1.Profile{ExternalId: req.Msg.ExternalId}}), nil
}

func (s *pugServer) Delete(context.Context, *connect.Request[profilesv1.DeleteRequest]) (*connect.Response[profilesv1.DeleteResponse], error) {
	return connect.NewResponse(&profilesv1.DeleteResponse{
		RequestId: proto.String("req_1"),
		Status:    profilesv1.ComplianceRequestStatus_COMPLIANCE_REQUEST_STATUS_PENDING.Enum(),
	}), nil
}

func (s *pugServer) List(_ context.Context, req *connect.Request[profilesv1.ListRequest], stream *connect.ServerStream[profilesv1.ListResponse]) error {
	s.saw(req.Header())
	s.mu.Lock()
	pages, listErr := s.listPages, s.listErr
	s.mu.Unlock()
	for _, page := range pages {
		if err := stream.Send(&profilesv1.ListResponse{Profiles: page}); err != nil {
			return err
		}
	}
	return listErr
}

type activityServer struct {
	activityv1connect.UnimplementedActivityServiceHandler
	*pugServer
}

type insightsServer struct {
	insightsv1connect.UnimplementedInsightsServiceHandler
	*pugServer
}

func (s activityServer) GetProfileStats(ctx context.Context, _ *connect.Request[activityv1.GetProfileStatsRequest]) (*connect.Response[activityv1.GetProfileStatsResponse], error) {
	s.mu.Lock()
	delay := s.statsDelay
	s.mu.Unlock()
	select {
	case <-time.After(delay):
		return connect.NewResponse(&activityv1.GetProfileStatsResponse{}), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (insightsServer) GetFilterSchema(context.Context, *connect.Request[commonv1.GetFilterSchemaRequest]) (*connect.Response[commonv1.GetFilterSchemaResponse], error) {
	return connect.NewResponse(&commonv1.GetFilterSchemaResponse{Events: []*commonv1.EventNameMeta{{Name: proto.String("purchase")}}}), nil
}

func (insightsServer) Query(context.Context, *connect.Request[insightsv1.QueryRequest]) (*connect.Response[insightsv1.QueryResponse], error) {
	return connect.NewResponse(&insightsv1.QueryResponse{Result: &insightsv1.QueryResponse_Trends{Trends: &insightsv1.TrendsResult{}}}), nil
}

func startServer(t *testing.T, srv *pugServer, opts Options) (*Client, *logs) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(eventsv1connect.NewEventsServiceHandler(srv))
	if srv.legacy {
		mux.Handle(sdkprofilesv1connect.ProfilesSDKServiceIdentifyProcedure,
			connect.NewUnaryHandler(sdkprofilesv1connect.ProfilesSDKServiceIdentifyProcedure, srv.Identify))
	} else {
		mux.Handle(sdkprofilesv1connect.NewProfilesSDKServiceHandler(srv))
	}
	mux.Handle(profilesv1connect.NewProfilesServiceHandler(srv))
	mux.Handle(activityv1connect.NewActivityServiceHandler(activityServer{pugServer: srv}))
	mux.Handle(insightsv1connect.NewInsightsServiceHandler(insightsServer{pugServer: srv}))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	l := &logs{}
	opts.APIKey = "prv_test"
	opts.Endpoint = ts.URL
	opts.Logger = slog.New(l)
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c, l
}

func TestE2ETrackDelivers(t *testing.T) {
	srv := &pugServer{}
	c, l := startServer(t, srv, Options{})
	c.Track("user_42", "order.completed", Properties{"amount": 49.0, "currency": "USD"})
	c.Track("user_42", "feature_used", Properties{"featureId": "export"})
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(srv.batches) != 1 || len(srv.batches[0]) != 2 {
		t.Fatalf("server got %v; logs %v", srv.batches, l.records)
	}
	if got := srv.batches[0][1]; got.GetKind() != "feature_used" || got.GetCustomProperties()["featureId"].GetStringValue() != "export" {
		t.Errorf("event = %v", got)
	}
	h := srv.headers[0]
	if h.Get("X-Api-Key") != "prv_test" || h.Get("Content-Type") != "application/proto" {
		t.Errorf("headers = %v", h)
	}
}

func TestE2EIdentify(t *testing.T) {
	srv := &pugServer{}
	c, l := startServer(t, srv, Options{})
	c.Identify("user_42", Email("ada@example.com"), String("plan", "pro"), WithAnonymousID("anon-1"))
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(srv.identifies) != 1 {
		t.Fatalf("server got %d identifies; logs %v", len(srv.identifies), l.records)
	}
	req := srv.identifies[0]
	traits := req.GetTraits().AsMap()
	if req.GetExternalId() != "user_42" || req.GetAnonymousId() != "anon-1" || traits["plan"] != "pro" || traits["email"] != "ada@example.com" {
		t.Errorf("request = %v", req)
	}
	if srv.headers[0].Get("X-Api-Key") != "prv_test" {
		t.Errorf("headers = %v", srv.headers[0])
	}
}

func TestE2EIdentifyOnAServerWithoutBatchIdentify(t *testing.T) {
	srv := &pugServer{legacy: true}
	c, l := startServer(t, srv, Options{})
	c.Identify("user_1")
	c.Identify("user_2")
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := externalIDs(srv.identifies); got != "user_1,user_2" {
		t.Errorf("server got %q; logs %v", got, l.records)
	}
	n := 0
	for _, r := range l.warns() {
		if strings.Contains(r.msg, "no BatchIdentify") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("logged the fallback %d times, want once: %v", n, l.records)
	}
}

func TestE2EReads(t *testing.T) {
	srv := &pugServer{}
	c, _ := startServer(t, srv, Options{})
	ctx := t.Context()

	if p, err := c.Profiles.Get(ctx, "p1"); err != nil || p.GetId() != "p1" {
		t.Errorf("Get = %v, %v", p, err)
	}
	if p, err := c.Profiles.GetByExternalID(ctx, "user_42", IncludeBots()); err != nil || p.GetExternalId() != "user_42" || !srv.lastGet.GetIncludeBots() {
		t.Errorf("GetByExternalID = %v, %v; server got %v", p, err, srv.lastGet)
	}
	if res, err := c.Profiles.Delete(ctx, "p1"); err != nil || res.GetRequestId() != "req_1" {
		t.Errorf("Delete = %v, %v", res, err)
	}
	if res, err := c.Insights.Query(ctx, &insightsv1.QueryRequest{}); err != nil || res.GetTrends() == nil {
		t.Errorf("Query = %v, %v", res, err)
	}
	if res, err := c.Insights.FilterSchema(ctx, nil); err != nil || len(res.GetEvents()) != 1 {
		t.Errorf("FilterSchema = %v, %v", res, err)
	}
}

func TestE2EReadErrors(t *testing.T) {
	c, _ := startServer(t, &pugServer{}, Options{})
	_, err := c.Profiles.Get(t.Context(), "missing")
	var perr *Error
	if !errors.As(err, &perr) || perr.Code != connect.CodeNotFound || perr.Message != "profile not found" {
		t.Fatalf("err = %#v", err)
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("connect.CodeOf = %v", connect.CodeOf(err))
	}
	_, err = c.Activity.Feed(t.Context(), nil)
	if !errors.As(err, &perr) || perr.Code != connect.CodeUnimplemented {
		t.Errorf("err = %#v", err)
	}
}

func profileIDs(t *testing.T, seq func(func(*profilesv1.Profile, error) bool), limit int) ([]string, error) {
	t.Helper()
	var got []string
	for p, err := range seq {
		if err != nil {
			return got, err
		}
		got = append(got, p.GetId())
		if len(got) == limit {
			break
		}
	}
	return got, nil
}

func page(ids ...string) []*profilesv1.Profile {
	out := make([]*profilesv1.Profile, len(ids))
	for i, id := range ids {
		out[i] = &profilesv1.Profile{Id: proto.String(id)}
	}
	return out
}

func TestE2EListPaginates(t *testing.T) {
	srv := &pugServer{listPages: [][]*profilesv1.Profile{page("p1", "p2"), page("p3")}}
	c, _ := startServer(t, srv, Options{})
	got, err := profileIDs(t, c.Profiles.List(t.Context(), nil), 0)
	if err != nil || len(got) != 3 || got[2] != "p3" {
		t.Errorf("List = %v, %v", got, err)
	}
	if srv.headers[0].Get("X-Api-Key") != "prv_test" {
		t.Errorf("stream headers = %v", srv.headers[0])
	}
	got, err = profileIDs(t, c.Profiles.List(t.Context(), &profilesv1.ListRequest{}), 1)
	if err != nil || len(got) != 1 {
		t.Errorf("List with early break = %v, %v", got, err)
	}
}

func TestE2EListSurfacesStreamErrors(t *testing.T) {
	srv := &pugServer{listPages: [][]*profilesv1.Profile{page("p1")}, listErr: connect.NewError(connect.CodePermissionDenied, errors.New("denied"))}
	c, _ := startServer(t, srv, Options{})
	got, err := profileIDs(t, c.Profiles.List(t.Context(), nil), 0)
	var perr *Error
	if len(got) != 1 || !errors.As(err, &perr) || perr.Code != connect.CodePermissionDenied {
		t.Errorf("List = %v, %#v", got, err)
	}
}

func TestE2EDefaultTimeout(t *testing.T) {
	srv := &pugServer{statsDelay: time.Second}
	c, _ := startServer(t, srv, Options{Timeout: 50 * time.Millisecond})
	start := time.Now()
	_, err := c.Activity.ProfileStats(t.Context(), &activityv1.GetProfileStatsRequest{DistinctId: proto.String("p1")})
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded || time.Since(start) > 900*time.Millisecond {
		t.Errorf("ProfileStats = %v after %v, want the 50ms default to cut it", err, time.Since(start))
	}

	srv.mu.Lock()
	srv.statsDelay = 100 * time.Millisecond
	srv.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := c.Activity.ProfileStats(ctx, &activityv1.GetProfileStatsRequest{DistinctId: proto.String("p1")}); err != nil {
		t.Errorf("ProfileStats with the caller's deadline = %v", err)
	}
}

func TestE2EPermanentFailureDeadLetters(t *testing.T) {
	srv := &pugServer{batchErrs: []error{connect.NewError(connect.CodeInvalidArgument, errors.New("bad batch"))}}
	dl := &deadLetters{}
	c, _ := startServer(t, srv, Options{OnError: dl.record})
	c.Track("user_42", "my.custom")
	_ = c.Flush(t.Context())
	if got := dl.all(); len(got) != 1 || connect.CodeOf(got[0].err) != connect.CodeInvalidArgument {
		t.Errorf("dead letters = %v", got)
	}
}

func TestE2ETransientFailureRetries(t *testing.T) {
	srv := &pugServer{batchErrs: []error{connect.NewError(connect.CodeUnavailable, errors.New("down"))}}
	dl := &deadLetters{}
	c, _ := startServer(t, srv, Options{OnError: dl.record})
	c.Track("user_42", "my.custom")
	_ = c.Flush(t.Context())
	if len(srv.batches) != 0 {
		t.Fatalf("server got %v", srv.batches)
	}
	_ = c.Flush(t.Context())
	if len(srv.batches) != 1 || len(dl.all()) != 0 {
		t.Errorf("server got %v, dead letters %v", srv.batches, dl.all())
	}
}

type procedureRecorder struct {
	mu   sync.Mutex
	last string
}

func (r *procedureRecorder) record(p string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last = p
}

func (r *procedureRecorder) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		r.record(req.Spec().Procedure)
		return next(ctx, req)
	}
}

func (r *procedureRecorder) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (r *procedureRecorder) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		r.record(conn.Spec().Procedure)
		return next(ctx, conn)
	}
}

func TestE2EReadsHitTheirProcedures(t *testing.T) {
	rec := &procedureRecorder{}
	opt := connect.WithInterceptors(rec)
	mux := http.NewServeMux()
	mux.Handle(profilesv1connect.NewProfilesServiceHandler(profilesv1connect.UnimplementedProfilesServiceHandler{}, opt))
	mux.Handle(activityv1connect.NewActivityServiceHandler(activityv1connect.UnimplementedActivityServiceHandler{}, opt))
	mux.Handle(insightsv1connect.NewInsightsServiceHandler(insightsv1connect.UnimplementedInsightsServiceHandler{}, opt))
	ts := httptest.NewServer(mux)
	defer ts.Close()
	c, err := New(Options{APIKey: "prv_test", Endpoint: ts.URL, Logger: slog.New(&logs{})})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(t.Context())

	ctx := t.Context()
	for _, tc := range []struct {
		procedure string
		call      func() error
	}{
		{profilesv1connect.ProfilesServiceGetProcedure, func() error { _, err := c.Profiles.Get(ctx, "p1"); return err }},
		{profilesv1connect.ProfilesServiceGetByExternalIdProcedure, func() error { _, err := c.Profiles.GetByExternalID(ctx, "u1"); return err }},
		{profilesv1connect.ProfilesServiceDeleteProcedure, func() error { _, err := c.Profiles.Delete(ctx, "p1"); return err }},
		{profilesv1connect.ProfilesServiceListProcedure, func() error {
			for _, err := range c.Profiles.List(ctx, nil) {
				return err
			}
			return nil
		}},
		{activityv1connect.ActivityServiceGetActivityFeedProcedure, func() error { _, err := c.Activity.Feed(ctx, nil); return err }},
		{activityv1connect.ActivityServiceGetEventExplorerProcedure, func() error { _, err := c.Activity.EventExplorer(ctx, nil); return err }},
		{activityv1connect.ActivityServiceGetActivityHeatmapProcedure, func() error { _, err := c.Activity.Heatmap(ctx, nil); return err }},
		{activityv1connect.ActivityServiceGetProfileSessionsProcedure, func() error { _, err := c.Activity.ProfileSessions(ctx, nil); return err }},
		{activityv1connect.ActivityServiceGetProfileStatsProcedure, func() error { _, err := c.Activity.ProfileStats(ctx, nil); return err }},
		{activityv1connect.ActivityServiceGetFilterSchemaProcedure, func() error { _, err := c.Activity.FilterSchema(ctx, nil); return err }},
		{activityv1connect.ActivityServiceGetPropertyValuesProcedure, func() error { _, err := c.Activity.PropertyValues(ctx, nil); return err }},
		{insightsv1connect.InsightsServiceQueryProcedure, func() error { _, err := c.Insights.Query(ctx, nil); return err }},
		{insightsv1connect.InsightsServiceSegmentUsersProcedure, func() error { _, err := c.Insights.SegmentUsers(ctx, nil); return err }},
		{insightsv1connect.InsightsServiceGetFilterSchemaProcedure, func() error { _, err := c.Insights.FilterSchema(ctx, nil); return err }},
		{insightsv1connect.InsightsServiceGetPropertyValuesProcedure, func() error { _, err := c.Insights.PropertyValues(ctx, nil); return err }},
	} {
		err := tc.call()
		if connect.CodeOf(err) != connect.CodeUnimplemented {
			t.Errorf("%s: err = %v, want the server's unimplemented", tc.procedure, err)
		}
		rec.mu.Lock()
		got := rec.last
		rec.mu.Unlock()
		if got != tc.procedure {
			t.Errorf("hit %s, want %s", got, tc.procedure)
		}
	}
}
