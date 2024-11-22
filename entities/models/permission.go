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

// Permission permissions attached to a role.
//
// swagger:model Permission
type Permission struct {

	// allowed actions in weaviate.
	// Required: true
	// Enum: [manage_roles read_roles manage_cluster create_collections read_collections update_collections delete_collections create_tenants read_tenants update_tenants delete_tenants create_objects_collection read_objects_collection update_objects_collection delete_objects_collection create_objects_tenant read_objects_tenant update_objects_tenant delete_objects_tenant]
	Action *string `json:"action"`

	// string or regex. if a specific collection name, if left empty it will be ALL or *
	Collection *string `json:"collection,omitempty"`

	// string or regex. if a specific object ID, if left empty it will be ALL or *
	Object *string `json:"object,omitempty"`

	// string or regex. if a specific role name, if left empty it will be ALL or *
	Role *string `json:"role,omitempty"`

	// string or regex. if a specific tenant name, if left empty it will be ALL or *
	Tenant *string `json:"tenant,omitempty"`
}

// Validate validates this permission
func (m *Permission) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAction(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var permissionTypeActionPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["manage_roles","read_roles","manage_cluster","create_collections","read_collections","update_collections","delete_collections","create_tenants","read_tenants","update_tenants","delete_tenants","create_objects_collection","read_objects_collection","update_objects_collection","delete_objects_collection","create_objects_tenant","read_objects_tenant","update_objects_tenant","delete_objects_tenant"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		permissionTypeActionPropEnum = append(permissionTypeActionPropEnum, v)
	}
}

const (

	// PermissionActionManageRoles captures enum value "manage_roles"
	PermissionActionManageRoles string = "manage_roles"

	// PermissionActionReadRoles captures enum value "read_roles"
	PermissionActionReadRoles string = "read_roles"

	// PermissionActionManageCluster captures enum value "manage_cluster"
	PermissionActionManageCluster string = "manage_cluster"

	// PermissionActionCreateCollections captures enum value "create_collections"
	PermissionActionCreateCollections string = "create_collections"

	// PermissionActionReadCollections captures enum value "read_collections"
	PermissionActionReadCollections string = "read_collections"

	// PermissionActionUpdateCollections captures enum value "update_collections"
	PermissionActionUpdateCollections string = "update_collections"

	// PermissionActionDeleteCollections captures enum value "delete_collections"
	PermissionActionDeleteCollections string = "delete_collections"

	// PermissionActionCreateTenants captures enum value "create_tenants"
	PermissionActionCreateTenants string = "create_tenants"

	// PermissionActionReadTenants captures enum value "read_tenants"
	PermissionActionReadTenants string = "read_tenants"

	// PermissionActionUpdateTenants captures enum value "update_tenants"
	PermissionActionUpdateTenants string = "update_tenants"

	// PermissionActionDeleteTenants captures enum value "delete_tenants"
	PermissionActionDeleteTenants string = "delete_tenants"

	// PermissionActionCreateObjectsCollection captures enum value "create_objects_collection"
	PermissionActionCreateObjectsCollection string = "create_objects_collection"

	// PermissionActionReadObjectsCollection captures enum value "read_objects_collection"
	PermissionActionReadObjectsCollection string = "read_objects_collection"

	// PermissionActionUpdateObjectsCollection captures enum value "update_objects_collection"
	PermissionActionUpdateObjectsCollection string = "update_objects_collection"

	// PermissionActionDeleteObjectsCollection captures enum value "delete_objects_collection"
	PermissionActionDeleteObjectsCollection string = "delete_objects_collection"

	// PermissionActionCreateObjectsTenant captures enum value "create_objects_tenant"
	PermissionActionCreateObjectsTenant string = "create_objects_tenant"

	// PermissionActionReadObjectsTenant captures enum value "read_objects_tenant"
	PermissionActionReadObjectsTenant string = "read_objects_tenant"

	// PermissionActionUpdateObjectsTenant captures enum value "update_objects_tenant"
	PermissionActionUpdateObjectsTenant string = "update_objects_tenant"

	// PermissionActionDeleteObjectsTenant captures enum value "delete_objects_tenant"
	PermissionActionDeleteObjectsTenant string = "delete_objects_tenant"
)

// prop value enum
func (m *Permission) validateActionEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, permissionTypeActionPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *Permission) validateAction(formats strfmt.Registry) error {

	if err := validate.Required("action", "body", m.Action); err != nil {
		return err
	}

	// value enum
	if err := m.validateActionEnum("action", "body", *m.Action); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this permission based on context it is used
func (m *Permission) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *Permission) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Permission) UnmarshalBinary(b []byte) error {
	var res Permission
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
