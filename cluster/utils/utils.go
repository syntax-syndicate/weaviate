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

package utils

import (
	"fmt"
	"net"
	"slices"
	"sync"
)

func MustGetFreeTCPPort() (port int) {
	lAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	l, err := net.ListenTCP("tcp", lAddr)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type ClassTenantDataVersion struct {
	ClassName         string
	TenantName        string
	TenantDataVersion uint64
}

type QuerierSubscribers struct {
	m                    sync.Mutex
	querierSubscriptions map[chan ClassTenantDataVersion]struct{}
	// nodeTenantDataVersions is a map of nodeID -> tenant -> version -> struct{}
	// nodeTenantDataVersions   map[string]map[string]map[uint64]struct{}
	nodeTenantDataVersionsV2 []ClassTenantDataVersion
}

func NewQuerierSubscribers() *QuerierSubscribers {
	return &QuerierSubscribers{
		querierSubscriptions: map[chan ClassTenantDataVersion]struct{}{},
		// nodeTenantDataVersions:   make(map[string]map[string]map[uint64]struct{}),
		nodeTenantDataVersionsV2: []ClassTenantDataVersion{},
	}
}

func (qs *QuerierSubscribers) Subscribe(ch chan ClassTenantDataVersion) {
	qs.m.Lock()
	defer qs.m.Unlock()
	qs.querierSubscriptions[ch] = struct{}{}
}

func (qs *QuerierSubscribers) Unsubscribe(ch chan ClassTenantDataVersion) {
	qs.m.Lock()
	defer qs.m.Unlock()
	delete(qs.querierSubscriptions, ch)
}

func (qs *QuerierSubscribers) Notify(ctdv ClassTenantDataVersion) {
	fmt.Println("NOTE Telling registered query nodes that tenant has new version", len(qs.querierSubscriptions), ctdv.ClassName, ctdv.TenantName, ctdv.TenantDataVersion)
	qs.m.Lock()
	defer qs.m.Unlock()
	for ch := range qs.querierSubscriptions {
		ch <- ctdv
	}
}

func (qs *QuerierSubscribers) AddNodeTenantDataVersion(ctdv ClassTenantDataVersion) {
	fmt.Println("NOTE Received notice that querier node has tenant ready", ctdv.ClassName, ctdv.TenantName)
	qs.m.Lock()
	defer qs.m.Unlock()

	qs.nodeTenantDataVersionsV2 = append(qs.nodeTenantDataVersionsV2, ctdv)

	// if qs.nodeTenantDataVersions == nil {
	// 	qs.nodeTenantDataVersions = make(map[string]map[string]map[uint64]struct{})
	// }
	// if _, ok := qs.nodeTenantDataVersions[nodeID]; !ok {
	// 	qs.nodeTenantDataVersions[nodeID] = make(map[string]map[uint64]struct{})
	// }
	// if _, ok := qs.nodeTenantDataVersions[nodeID][tenant]; !ok {
	// 	qs.nodeTenantDataVersions[nodeID][tenant] = make(map[uint64]struct{})
	// }
	// qs.nodeTenantDataVersions[nodeID][tenant][version] = struct{}{}
}

func (qs *QuerierSubscribers) RemoveNodeTenantDataVersion(ctdv ClassTenantDataVersion) {
	qs.m.Lock()
	defer qs.m.Unlock()

	// TODO horribly slow, replace with maps or be fancy or something?
	i := slices.Index(qs.nodeTenantDataVersionsV2, ctdv)
	if i != -1 {
		qs.nodeTenantDataVersionsV2 = append(qs.nodeTenantDataVersionsV2[:i], qs.nodeTenantDataVersionsV2[i+1:]...)
	}

	// if _, ok := qs.nodeTenantDataVersions[nodeID]; ok {
	// 	if _, ok := qs.nodeTenantDataVersions[nodeID][tenant]; ok {
	// 		delete(qs.nodeTenantDataVersions[nodeID][tenant], version)
	// 	}
	// }
}
