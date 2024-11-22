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
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// NestedProperty nested property
//
// swagger:model NestedProperty
type NestedProperty struct {

	// data type
	DataType []string `json:"dataType"`

	// description
	Description string `json:"description,omitempty"`

	// index filterable
	IndexFilterable *bool `json:"indexFilterable,omitempty"`

	// index range filters
	IndexRangeFilters *bool `json:"indexRangeFilters,omitempty"`

	// index searchable
	IndexSearchable *bool `json:"indexSearchable,omitempty"`

	// name
	Name string `json:"name,omitempty"`

	// The properties of the nested object(s). Applies to object and object[] data types.
	NestedProperties []*NestedProperty `json:"nestedProperties,omitempty"`

	// tokenization
	// Enum: [word lowercase whitespace field trigram gse kagome_kr kagome_ja]
	Tokenization string `json:"tokenization,omitempty"`
}

// Validate validates this nested property
func (m *NestedProperty) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateNestedProperties(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTokenization(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *NestedProperty) validateNestedProperties(formats strfmt.Registry) error {
	if swag.IsZero(m.NestedProperties) { // not required
		return nil
	}

	for i := 0; i < len(m.NestedProperties); i++ {
		if swag.IsZero(m.NestedProperties[i]) { // not required
			continue
		}

		if m.NestedProperties[i] != nil {
			if err := m.NestedProperties[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("nestedProperties" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("nestedProperties" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

var nestedPropertyTypeTokenizationPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["word","lowercase","whitespace","field","trigram","gse","kagome_kr","kagome_ja"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		nestedPropertyTypeTokenizationPropEnum = append(nestedPropertyTypeTokenizationPropEnum, v)
	}
}

const (

	// NestedPropertyTokenizationWord captures enum value "word"
	NestedPropertyTokenizationWord string = "word"

	// NestedPropertyTokenizationLowercase captures enum value "lowercase"
	NestedPropertyTokenizationLowercase string = "lowercase"

	// NestedPropertyTokenizationWhitespace captures enum value "whitespace"
	NestedPropertyTokenizationWhitespace string = "whitespace"

	// NestedPropertyTokenizationField captures enum value "field"
	NestedPropertyTokenizationField string = "field"

	// NestedPropertyTokenizationTrigram captures enum value "trigram"
	NestedPropertyTokenizationTrigram string = "trigram"

	// NestedPropertyTokenizationGse captures enum value "gse"
	NestedPropertyTokenizationGse string = "gse"

	// NestedPropertyTokenizationKagomeKr captures enum value "kagome_kr"
	NestedPropertyTokenizationKagomeKr string = "kagome_kr"

	// NestedPropertyTokenizationKagomeJa captures enum value "kagome_ja"
	NestedPropertyTokenizationKagomeJa string = "kagome_ja"
)

// prop value enum
func (m *NestedProperty) validateTokenizationEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, nestedPropertyTypeTokenizationPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *NestedProperty) validateTokenization(formats strfmt.Registry) error {
	if swag.IsZero(m.Tokenization) { // not required
		return nil
	}

	// value enum
	if err := m.validateTokenizationEnum("tokenization", "body", m.Tokenization); err != nil {
		return err
	}

	return nil
}

// ContextValidate validate this nested property based on the context it is used
func (m *NestedProperty) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateNestedProperties(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *NestedProperty) contextValidateNestedProperties(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(m.NestedProperties); i++ {

		if m.NestedProperties[i] != nil {
			if err := m.NestedProperties[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("nestedProperties" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("nestedProperties" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *NestedProperty) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *NestedProperty) UnmarshalBinary(b []byte) error {
	var res NestedProperty
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
