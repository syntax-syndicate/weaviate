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
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	protoapi "github.com/weaviate/weaviate/cluster/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// v1/schema/collection/tenants
	DefaultSchemaPath = "v1/schema/%s/tenants"
)

var ErrTenantNotFound = errors.New("tenant not found")

type TenantInfo struct {
	addr   string
	path   string
	client *http.Client
}

func NewTenantInfo(addr, path string) *TenantInfo {
	c := http.DefaultClient
	c.Timeout = 2 * time.Second

	return &TenantInfo{
		addr:   addr,
		path:   path,
		client: c,
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

func (t *TenantInfo) TenantDataVersion(ctx context.Context, collection, tenant string) (uint64, error) {
	outboundIp := getOutboundIP()
	leaderRpcConn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:8301", outboundIp), grpc.WithTransportCredentials(insecure.NewCredentials())) //, options...)
	if err != nil {
		return 0, nil
	}
	c := protoapi.NewClusterServiceClient(leaderRpcConn)
	r, err := c.GetTenantDataVersion(ctx, &protoapi.GetTenantDataVersionRequest{})
	if err != nil {
		return 0, nil
	}
	return r.Version, nil
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
