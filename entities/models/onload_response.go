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

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// OnloadResponse The definition of a onload response body
//
// swagger:model OnloadResponse
type OnloadResponse struct {

	// Onload backend name e.g. filesystem, gcs, s3.
	Backend string `json:"backend,omitempty"`

	// Class (name) onloaded tenants belong to
	Class string `json:"class,omitempty"`

	// error message if onload failed
	Error string `json:"error,omitempty"`

	// destination path of onload files proper to selected backend
	Path string `json:"path,omitempty"`

	// phase of onload process
	// Enum: [STARTED TRANSFERRING TRANSFERRED SUCCESS FAILED]
	Status *string `json:"status,omitempty"`

	// The list of classes for which the onload process was started
	Tenants []string `json:"tenants"`
}

// Validate validates this onload response
func (m *OnloadResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var onloadResponseTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["STARTED","TRANSFERRING","TRANSFERRED","SUCCESS","FAILED"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		onloadResponseTypeStatusPropEnum = append(onloadResponseTypeStatusPropEnum, v)
	}
}

const (

	// OnloadResponseStatusSTARTED captures enum value "STARTED"
	OnloadResponseStatusSTARTED string = "STARTED"

	// OnloadResponseStatusTRANSFERRING captures enum value "TRANSFERRING"
	OnloadResponseStatusTRANSFERRING string = "TRANSFERRING"

	// OnloadResponseStatusTRANSFERRED captures enum value "TRANSFERRED"
	OnloadResponseStatusTRANSFERRED string = "TRANSFERRED"

	// OnloadResponseStatusSUCCESS captures enum value "SUCCESS"
	OnloadResponseStatusSUCCESS string = "SUCCESS"

	// OnloadResponseStatusFAILED captures enum value "FAILED"
	OnloadResponseStatusFAILED string = "FAILED"
)

// prop value enum
func (m *OnloadResponse) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, onloadResponseTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *OnloadResponse) validateStatus(formats strfmt.Registry) error {
	if swag.IsZero(m.Status) { // not required
		return nil
	}

	// value enum
	if err := m.validateStatusEnum("status", "body", *m.Status); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this onload response based on context it is used
func (m *OnloadResponse) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *OnloadResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *OnloadResponse) UnmarshalBinary(b []byte) error {
	var res OnloadResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
