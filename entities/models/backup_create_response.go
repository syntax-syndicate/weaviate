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

// BackupCreateResponse The definition of a backup create response body
//
// swagger:model BackupCreateResponse
type BackupCreateResponse struct {

	// Backup backend name e.g. filesystem, gcs, s3.
	Backend string `json:"backend,omitempty"`

	// Name of the S3 bucket
	Bucket string `json:"bucket,omitempty"`

	// The list of classes for which the backup creation process was started
	Classes []string `json:"classes"`

	// error message if creation failed
	Error string `json:"error,omitempty"`

	// The ID of the backup. Must be URL-safe and work as a filesystem path, only lowercase, numbers, underscore, minus characters allowed.
	ID string `json:"id,omitempty"`

	// destination path of backup files proper to selected backend
	Path string `json:"path,omitempty"`

	// phase of backup creation process
	// Enum: [STARTED TRANSFERRING TRANSFERRED SUCCESS FAILED]
	Status *string `json:"status,omitempty"`
}

// Validate validates this backup create response
func (m *BackupCreateResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var backupCreateResponseTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["STARTED","TRANSFERRING","TRANSFERRED","SUCCESS","FAILED"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		backupCreateResponseTypeStatusPropEnum = append(backupCreateResponseTypeStatusPropEnum, v)
	}
}

const (

	// BackupCreateResponseStatusSTARTED captures enum value "STARTED"
	BackupCreateResponseStatusSTARTED string = "STARTED"

	// BackupCreateResponseStatusTRANSFERRING captures enum value "TRANSFERRING"
	BackupCreateResponseStatusTRANSFERRING string = "TRANSFERRING"

	// BackupCreateResponseStatusTRANSFERRED captures enum value "TRANSFERRED"
	BackupCreateResponseStatusTRANSFERRED string = "TRANSFERRED"

	// BackupCreateResponseStatusSUCCESS captures enum value "SUCCESS"
	BackupCreateResponseStatusSUCCESS string = "SUCCESS"

	// BackupCreateResponseStatusFAILED captures enum value "FAILED"
	BackupCreateResponseStatusFAILED string = "FAILED"
)

// prop value enum
func (m *BackupCreateResponse) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, backupCreateResponseTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *BackupCreateResponse) validateStatus(formats strfmt.Registry) error {
	if swag.IsZero(m.Status) { // not required
		return nil
	}

	// value enum
	if err := m.validateStatusEnum("status", "body", *m.Status); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this backup create response based on context it is used
func (m *BackupCreateResponse) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *BackupCreateResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *BackupCreateResponse) UnmarshalBinary(b []byte) error {
	var res BackupCreateResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
