package pug_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	pug "github.com/pug-sh/sdk-go"
	commoneventsv1 "github.com/pug-sh/sdk-go/gen/common/events/v1"
)

func Example() {
	client, err := pug.New(pug.Options{
		APIKey:   os.Getenv("PUG_API_KEY"),
		Endpoint: "https://pug.example.com",
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	client.Track("user_42", "order.completed", pug.Float("amount", 49.0), pug.String("currency", "USD"))
	client.TrackWellKnown("user_42", &commoneventsv1.FeatureUsedProperties{FeatureId: proto.String("export")})
	client.Identify(ctx, "user_42", pug.Traits{"email": "ada@example.com", "plan": "pro"})

	profile, err := client.Profiles.GetByExternalID(ctx, "user_42")
	if err != nil {
		log.Print(err)
	}
	fmt.Println(profile.GetId())

	if err := client.Close(ctx); err != nil {
		log.Print(err)
	}
}

func ExampleClient_Track() {
	client, _ := pug.New(pug.Options{APIKey: os.Getenv("PUG_API_KEY")})
	defer client.Close(context.Background())

	client.Track("user_123", "checkout_completed", pug.String("plan", "pro"),
		pug.WithLocation(pug.Location{Country: "DE", City: "Berlin"}),
		pug.WithTimestamp(time.Now().Add(-time.Minute)),
	)
}

func ExampleKind() {
	client, _ := pug.New(pug.Options{APIKey: os.Getenv("PUG_API_KEY")})
	defer client.Close(context.Background())

	kind := os.Getenv("CHECKOUT_EVENT") // a kind held in a variable needs the conversion
	client.Track("user_42", pug.Kind(kind), pug.Float("amount", 49))
}

func ExampleClient_TrackWellKnown() {
	client, _ := pug.New(pug.Options{APIKey: os.Getenv("PUG_API_KEY")})
	defer client.Close(context.Background())

	client.TrackWellKnown("user_42", &commoneventsv1.PurchaseProperties{
		Amount:   proto.Float64(49),
		Currency: proto.String("USD"),
		OrderId:  proto.String("ord_1"),
	}, pug.String("coupon", "FALL25"))
}

func ExampleClient_Close() {
	client, _ := pug.New(pug.Options{
		APIKey: os.Getenv("PUG_API_KEY"),
		OnError: func(err error, events []*pug.Event) {
			log.Printf("dropping %d event(s): %v", len(events), err)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		log.Print(err)
	}
}

func ExampleProfilesClient_List() {
	client, _ := pug.New(pug.Options{APIKey: os.Getenv("PUG_API_KEY")})
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for profile, err := range client.Profiles.List(ctx, nil) {
		if err != nil {
			log.Print(err)
			break
		}
		fmt.Println(profile.GetExternalId())
	}
}
