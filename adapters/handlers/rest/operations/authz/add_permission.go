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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/entities/models"
)

// AddPermissionHandlerFunc turns a function with the right signature into a add permission handler
type AddPermissionHandlerFunc func(AddPermissionParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn AddPermissionHandlerFunc) Handle(params AddPermissionParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// AddPermissionHandler interface for that can handle valid add permission params
type AddPermissionHandler interface {
	Handle(AddPermissionParams, *models.Principal) middleware.Responder
}

// NewAddPermission creates a new http.Handler for the add permission operation
func NewAddPermission(ctx *middleware.Context, handler AddPermissionHandler) *AddPermission {
	return &AddPermission{Context: ctx, Handler: handler}
}

/*
	AddPermission swagger:route POST /authz/roles/add-permission authz addPermission

Add permission to a role
*/
type AddPermission struct {
	Context *middleware.Context
	Handler AddPermissionHandler
}

func (o *AddPermission) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewAddPermissionParams()
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

// AddPermissionBody add permission body
//
// swagger:model AddPermissionBody
type AddPermissionBody struct {

	// name
	Name interface{} `json:"name,omitempty" yaml:"name,omitempty"`

	// permissions
	Permissions []*models.Permission `json:"permissions" yaml:"permissions"`
}

// Validate validates this add permission body
func (o *AddPermissionBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validatePermissions(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *AddPermissionBody) validatePermissions(formats strfmt.Registry) error {
	if swag.IsZero(o.Permissions) { // not required
		return nil
	}

	for i := 0; i < len(o.Permissions); i++ {
		if swag.IsZero(o.Permissions[i]) { // not required
			continue
		}

		if o.Permissions[i] != nil {
			if err := o.Permissions[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this add permission body based on the context it is used
func (o *AddPermissionBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidatePermissions(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *AddPermissionBody) contextValidatePermissions(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Permissions); i++ {

		if o.Permissions[i] != nil {
			if err := o.Permissions[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *AddPermissionBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *AddPermissionBody) UnmarshalBinary(b []byte) error {
	var res AddPermissionBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
