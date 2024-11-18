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

// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// TenantExistsHandlerFunc turns a function with the right signature into a tenant exists handler
type TenantExistsHandlerFunc func(TenantExistsParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn TenantExistsHandlerFunc) Handle(params TenantExistsParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// TenantExistsHandler interface for that can handle valid tenant exists params
type TenantExistsHandler interface {
	Handle(TenantExistsParams, *models.Principal) middleware.Responder
}

// NewTenantExists creates a new http.Handler for the tenant exists operation
func NewTenantExists(ctx *middleware.Context, handler TenantExistsHandler) *TenantExists {
	return &TenantExists{Context: ctx, Handler: handler}
}

/*
	TenantExists swagger:route HEAD /schema/{className}/tenants/{tenantName} schema tenantExists

# Check whether a tenant exists

Check if a tenant exists for a specific class
*/
type TenantExists struct {
	Context *middleware.Context
	Handler TenantExistsHandler
}

func (o *TenantExists) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewTenantExistsParams()
	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		*r = *aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request
	o.Context.Respond(rw, r, route.Produces, route, res)

}
