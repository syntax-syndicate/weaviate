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
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// GetRoleOKCode is the HTTP code returned for type GetRoleOK
const GetRoleOKCode int = 200

/*
GetRoleOK Successful response.

swagger:response getRoleOK
*/
type GetRoleOK struct {

	/*
	  In: Body
	*/
	Payload models.RolesListResponse `json:"body,omitempty"`
}

// NewGetRoleOK creates GetRoleOK with default headers values
func NewGetRoleOK() *GetRoleOK {

	return &GetRoleOK{}
}

// WithPayload adds the payload to the get role o k response
func (o *GetRoleOK) WithPayload(payload models.RolesListResponse) *GetRoleOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get role o k response
func (o *GetRoleOK) SetPayload(payload models.RolesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetRoleOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if payload == nil {
		// return empty array
		payload = models.RolesListResponse{}
	}

	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}
}

// GetRoleBadRequestCode is the HTTP code returned for type GetRoleBadRequest
const GetRoleBadRequestCode int = 400

/*
GetRoleBadRequest Malformed request.

swagger:response getRoleBadRequest
*/
type GetRoleBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetRoleBadRequest creates GetRoleBadRequest with default headers values
func NewGetRoleBadRequest() *GetRoleBadRequest {

	return &GetRoleBadRequest{}
}

// WithPayload adds the payload to the get role bad request response
func (o *GetRoleBadRequest) WithPayload(payload *models.ErrorResponse) *GetRoleBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get role bad request response
func (o *GetRoleBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetRoleBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GetRoleUnauthorizedCode is the HTTP code returned for type GetRoleUnauthorized
const GetRoleUnauthorizedCode int = 401

/*
GetRoleUnauthorized Unauthorized or invalid credentials.

swagger:response getRoleUnauthorized
*/
type GetRoleUnauthorized struct {
}

// NewGetRoleUnauthorized creates GetRoleUnauthorized with default headers values
func NewGetRoleUnauthorized() *GetRoleUnauthorized {

	return &GetRoleUnauthorized{}
}

// WriteResponse to the client
func (o *GetRoleUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// GetRoleForbiddenCode is the HTTP code returned for type GetRoleForbidden
const GetRoleForbiddenCode int = 403

/*
GetRoleForbidden Forbidden

swagger:response getRoleForbidden
*/
type GetRoleForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetRoleForbidden creates GetRoleForbidden with default headers values
func NewGetRoleForbidden() *GetRoleForbidden {

	return &GetRoleForbidden{}
}

// WithPayload adds the payload to the get role forbidden response
func (o *GetRoleForbidden) WithPayload(payload *models.ErrorResponse) *GetRoleForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get role forbidden response
func (o *GetRoleForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetRoleForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// GetRoleNotFoundCode is the HTTP code returned for type GetRoleNotFound
const GetRoleNotFoundCode int = 404

/*
GetRoleNotFound no role found

swagger:response getRoleNotFound
*/
type GetRoleNotFound struct {
}

// NewGetRoleNotFound creates GetRoleNotFound with default headers values
func NewGetRoleNotFound() *GetRoleNotFound {

	return &GetRoleNotFound{}
}

// WriteResponse to the client
func (o *GetRoleNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// GetRoleInternalServerErrorCode is the HTTP code returned for type GetRoleInternalServerError
const GetRoleInternalServerErrorCode int = 500

/*
GetRoleInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response getRoleInternalServerError
*/
type GetRoleInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewGetRoleInternalServerError creates GetRoleInternalServerError with default headers values
func NewGetRoleInternalServerError() *GetRoleInternalServerError {

	return &GetRoleInternalServerError{}
}

// WithPayload adds the payload to the get role internal server error response
func (o *GetRoleInternalServerError) WithPayload(payload *models.ErrorResponse) *GetRoleInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the get role internal server error response
func (o *GetRoleInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GetRoleInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
