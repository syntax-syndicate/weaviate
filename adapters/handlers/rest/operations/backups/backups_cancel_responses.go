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

package backups

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// BackupsCancelNoContentCode is the HTTP code returned for type BackupsCancelNoContent
const BackupsCancelNoContentCode int = 204

/*
BackupsCancelNoContent Successfully deleted.

swagger:response backupsCancelNoContent
*/
type BackupsCancelNoContent struct {
}

// NewBackupsCancelNoContent creates BackupsCancelNoContent with default headers values
func NewBackupsCancelNoContent() *BackupsCancelNoContent {

	return &BackupsCancelNoContent{}
}

// WriteResponse to the client
func (o *BackupsCancelNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(204)
}

// BackupsCancelUnauthorizedCode is the HTTP code returned for type BackupsCancelUnauthorized
const BackupsCancelUnauthorizedCode int = 401

/*
BackupsCancelUnauthorized Unauthorized or invalid credentials.

swagger:response backupsCancelUnauthorized
*/
type BackupsCancelUnauthorized struct {
}

// NewBackupsCancelUnauthorized creates BackupsCancelUnauthorized with default headers values
func NewBackupsCancelUnauthorized() *BackupsCancelUnauthorized {

	return &BackupsCancelUnauthorized{}
}

// WriteResponse to the client
func (o *BackupsCancelUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// BackupsCancelForbiddenCode is the HTTP code returned for type BackupsCancelForbidden
const BackupsCancelForbiddenCode int = 403

/*
BackupsCancelForbidden Forbidden

swagger:response backupsCancelForbidden
*/
type BackupsCancelForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsCancelForbidden creates BackupsCancelForbidden with default headers values
func NewBackupsCancelForbidden() *BackupsCancelForbidden {

	return &BackupsCancelForbidden{}
}

// WithPayload adds the payload to the backups cancel forbidden response
func (o *BackupsCancelForbidden) WithPayload(payload *models.ErrorResponse) *BackupsCancelForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups cancel forbidden response
func (o *BackupsCancelForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsCancelForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BackupsCancelUnprocessableEntityCode is the HTTP code returned for type BackupsCancelUnprocessableEntity
const BackupsCancelUnprocessableEntityCode int = 422

/*
BackupsCancelUnprocessableEntity Invalid backup cancellation attempt.

swagger:response backupsCancelUnprocessableEntity
*/
type BackupsCancelUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsCancelUnprocessableEntity creates BackupsCancelUnprocessableEntity with default headers values
func NewBackupsCancelUnprocessableEntity() *BackupsCancelUnprocessableEntity {

	return &BackupsCancelUnprocessableEntity{}
}

// WithPayload adds the payload to the backups cancel unprocessable entity response
func (o *BackupsCancelUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *BackupsCancelUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups cancel unprocessable entity response
func (o *BackupsCancelUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsCancelUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BackupsCancelInternalServerErrorCode is the HTTP code returned for type BackupsCancelInternalServerError
const BackupsCancelInternalServerErrorCode int = 500

/*
BackupsCancelInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response backupsCancelInternalServerError
*/
type BackupsCancelInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsCancelInternalServerError creates BackupsCancelInternalServerError with default headers values
func NewBackupsCancelInternalServerError() *BackupsCancelInternalServerError {

	return &BackupsCancelInternalServerError{}
}

// WithPayload adds the payload to the backups cancel internal server error response
func (o *BackupsCancelInternalServerError) WithPayload(payload *models.ErrorResponse) *BackupsCancelInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups cancel internal server error response
func (o *BackupsCancelInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsCancelInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
