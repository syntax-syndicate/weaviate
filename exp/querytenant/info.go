//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package querytenant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	protoapi "github.com/weaviate/weaviate/cluster/proto/api"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// v1/schema/collection/tenants
	DefaultSchemaPath = "v1/schema/%s/tenants"
)

var ErrTenantNotFound = errors.New("tenant not found")

type TenantInfo struct {
	addr                    string
	path                    string
	client                  *http.Client
	tenantDataVersionEvents chan *protoapi.QuerierEvent
	offload                 *modsloads3.Module
}

func NewTenantInfo(addr, path string, tenantDataVersionEvents chan *protoapi.QuerierEvent, offload *modsloads3.Module) *TenantInfo {
	c := http.DefaultClient
	c.Timeout = 2 * time.Second

	return &TenantInfo{
		addr:                    addr,
		path:                    path,
		client:                  c,
		tenantDataVersionEvents: tenantDataVersionEvents,
		offload:                 offload,
	}
}

func (t *TenantInfo) TenantStatus(ctx context.Context, collection, tenant string) (string, error) {
	respPayload := []Response{}

	path := fmt.Sprintf(t.path, collection)
	u := fmt.Sprintf("%s/%s", t.addr, path)

	resp, err := t.client.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return "", err
	}

	var rerr error
	for _, v := range respPayload {
		for _, e := range v.Error {
			rerr = errors.Join(rerr, errors.New(e.Message))
		}
		if strings.EqualFold(v.Name, tenant) {
			return v.Status, nil
		}

	}

	if rerr != nil {
		return "", rerr
	}

	return "", ErrTenantNotFound
}

type Response struct {
	Error  []ErrorResponse `json:"error,omitempty"`
	Status string          `json:"activityStatus"`
	Name   string          `json:"name"`
}

type ErrorResponse struct {
	Message string `json:"message"`
}

func (t *TenantInfo) StartQuerierSubscription(ctx context.Context, schemaGRPCHost string, schemaGRPCPort int) error {
	if schemaGRPCHost == "" {
		schemaGRPCHost = getOutboundIP()
	}
	leaderRpcConn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", schemaGRPCHost, schemaGRPCPort), grpc.WithTransportCredentials(insecure.NewCredentials())) //, options...)
	if err != nil {
		return nil
	}
	c := protoapi.NewClusterServiceClient(leaderRpcConn)
	stream, err := c.QuerierSubscription(context.Background())
	if err != nil {
		return err
	}

	// TODOD need to think more about how this and server side stuff handles context, concurrency, timeouts, etc
	wg := sync.WaitGroup{}
	wg.Add(2)

	// process events from metadata
	go func() {
		fmt.Println("NOTE Start listening for events from metadata nodes")
		defer wg.Done()
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				fmt.Println("t.sqs EOF")
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive a note : %v", err)
				panic(err)
			}
			switch in.Type {
			case protoapi.ServerEvent_UNSPECIFIED:
				panic("unspecified")
			case protoapi.ServerEvent_NEW_TENANT_DATA_VERSION:
				// TODO could use this to decide if we should downlaod new data version or what
				fmt.Println("NOTE New tenant data version available", in.Class, in.Tenant, in.TenantDataVersion)
				err = t.offload.Download(context.TODO(), in.Class, in.Tenant, "weaviate-0")
				fmt.Println("NOTE New tenant data version downloaded", in.Class, in.Tenant, in.TenantDataVersion)
				fmt.Println("t.sqs dl done")
				if err != nil {
					fmt.Println("t.sqs dl err")
					panic(err)
				}
				fmt.Println("t.sqs dl stdve")
				t.SendTenantDataVersionEvents(&protoapi.QuerierEvent{
					Type:              protoapi.QuerierEvent_TENANT_DATA_VERSION_READY,
					ClassName:         in.Class,
					TenantName:        in.Tenant,
					TenantDataVersion: in.TenantDataVersion,
				})
				fmt.Println("t.sqs dl stdve done")
				fmt.Println("NOTE Notified metadata server that we've downloaded async", in.Class, in.Tenant, in.TenantDataVersion)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				stream.CloseSend()
				wg.Done()
				return
			case e := <-t.tenantDataVersionEvents:
				if err := stream.Send(e); err != nil {
					log.Fatalf("Failed to send a note: %v", err)
				}
			}
		}
	}()

	wg.Wait()

	stream.CloseSend()
	return nil
}

func (t *TenantInfo) SendTenantDataVersionEvents(e *protoapi.QuerierEvent) {
	t.tenantDataVersionEvents <- e
}

// Get preferred outbound ip of this machine
// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr()

	return strings.Split(localAddr.String(), ":")[0]
}
