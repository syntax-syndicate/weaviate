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

package users

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// DeactivateUserOKCode is the HTTP code returned for type DeactivateUserOK
const DeactivateUserOKCode int = 200

/*
DeactivateUserOK users successfully deactivated

swagger:response deactivateUserOK
*/
type DeactivateUserOK struct {
}

// NewDeactivateUserOK creates DeactivateUserOK with default headers values
func NewDeactivateUserOK() *DeactivateUserOK {

	return &DeactivateUserOK{}
}

// WriteResponse to the client
func (o *DeactivateUserOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// DeactivateUserBadRequestCode is the HTTP code returned for type DeactivateUserBadRequest
const DeactivateUserBadRequestCode int = 400

/*
DeactivateUserBadRequest Malformed request.

swagger:response deactivateUserBadRequest
*/
type DeactivateUserBadRequest struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewDeactivateUserBadRequest creates DeactivateUserBadRequest with default headers values
func NewDeactivateUserBadRequest() *DeactivateUserBadRequest {

	return &DeactivateUserBadRequest{}
}

// WithPayload adds the payload to the deactivate user bad request response
func (o *DeactivateUserBadRequest) WithPayload(payload *models.ErrorResponse) *DeactivateUserBadRequest {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the deactivate user bad request response
func (o *DeactivateUserBadRequest) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *DeactivateUserBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// DeactivateUserUnauthorizedCode is the HTTP code returned for type DeactivateUserUnauthorized
const DeactivateUserUnauthorizedCode int = 401

/*
DeactivateUserUnauthorized Unauthorized or invalid credentials.

swagger:response deactivateUserUnauthorized
*/
type DeactivateUserUnauthorized struct {
}

// NewDeactivateUserUnauthorized creates DeactivateUserUnauthorized with default headers values
func NewDeactivateUserUnauthorized() *DeactivateUserUnauthorized {

	return &DeactivateUserUnauthorized{}
}

// WriteResponse to the client
func (o *DeactivateUserUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// DeactivateUserForbiddenCode is the HTTP code returned for type DeactivateUserForbidden
const DeactivateUserForbiddenCode int = 403

/*
DeactivateUserForbidden Forbidden

swagger:response deactivateUserForbidden
*/
type DeactivateUserForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewDeactivateUserForbidden creates DeactivateUserForbidden with default headers values
func NewDeactivateUserForbidden() *DeactivateUserForbidden {

	return &DeactivateUserForbidden{}
}

// WithPayload adds the payload to the deactivate user forbidden response
func (o *DeactivateUserForbidden) WithPayload(payload *models.ErrorResponse) *DeactivateUserForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the deactivate user forbidden response
func (o *DeactivateUserForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *DeactivateUserForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// DeactivateUserNotFoundCode is the HTTP code returned for type DeactivateUserNotFound
const DeactivateUserNotFoundCode int = 404

/*
DeactivateUserNotFound user not found

swagger:response deactivateUserNotFound
*/
type DeactivateUserNotFound struct {
}

// NewDeactivateUserNotFound creates DeactivateUserNotFound with default headers values
func NewDeactivateUserNotFound() *DeactivateUserNotFound {

	return &DeactivateUserNotFound{}
}

// WriteResponse to the client
func (o *DeactivateUserNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(404)
}

// DeactivateUserUnprocessableEntityCode is the HTTP code returned for type DeactivateUserUnprocessableEntity
const DeactivateUserUnprocessableEntityCode int = 422

/*
DeactivateUserUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response deactivateUserUnprocessableEntity
*/
type DeactivateUserUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewDeactivateUserUnprocessableEntity creates DeactivateUserUnprocessableEntity with default headers values
func NewDeactivateUserUnprocessableEntity() *DeactivateUserUnprocessableEntity {

	return &DeactivateUserUnprocessableEntity{}
}

// WithPayload adds the payload to the deactivate user unprocessable entity response
func (o *DeactivateUserUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *DeactivateUserUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the deactivate user unprocessable entity response
func (o *DeactivateUserUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *DeactivateUserUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// DeactivateUserInternalServerErrorCode is the HTTP code returned for type DeactivateUserInternalServerError
const DeactivateUserInternalServerErrorCode int = 500

/*
DeactivateUserInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response deactivateUserInternalServerError
*/
type DeactivateUserInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewDeactivateUserInternalServerError creates DeactivateUserInternalServerError with default headers values
func NewDeactivateUserInternalServerError() *DeactivateUserInternalServerError {

	return &DeactivateUserInternalServerError{}
}

// WithPayload adds the payload to the deactivate user internal server error response
func (o *DeactivateUserInternalServerError) WithPayload(payload *models.ErrorResponse) *DeactivateUserInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the deactivate user internal server error response
func (o *DeactivateUserInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *DeactivateUserInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
