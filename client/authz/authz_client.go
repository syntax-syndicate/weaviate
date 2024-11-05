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
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new authz API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for authz API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientOption is the option for Client methods
type ClientOption func(*runtime.ClientOperation)

// ClientService is the interface for Client methods
type ClientService interface {
	AddPermission(params *AddPermissionParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddPermissionCreated, error)

	AssignRole(params *AssignRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AssignRoleOK, error)

	CreateRole(params *CreateRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*CreateRoleCreated, error)

	DeleteRole(params *DeleteRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*DeleteRoleNoContent, error)

	GetRole(params *GetRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRoleOK, error)

	GetRoles(params *GetRolesParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesOK, error)

	GetRolesForUser(params *GetRolesForUserParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesForUserOK, error)

	GetUsersForRole(params *GetUsersForRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetUsersForRoleOK, error)

	RemovedPermission(params *RemovedPermissionParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RemovedPermissionCreated, error)

	RevokeRole(params *RevokeRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RevokeRoleOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
AddPermission adds permission to a role it will be upsert if the role doesn t exists it will be created
*/
func (a *Client) AddPermission(params *AddPermissionParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddPermissionCreated, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewAddPermissionParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "addPermission",
		Method:             "POST",
		PathPattern:        "/authz/roles/add-permission",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &AddPermissionReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*AddPermissionCreated)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for addPermission: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
AssignRole assigns a role to a user or key
*/
func (a *Client) AssignRole(params *AssignRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AssignRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewAssignRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "assignRole",
		Method:             "POST",
		PathPattern:        "/authz/users/{id}/assign",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &AssignRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*AssignRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for assignRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
CreateRole creates new role
*/
func (a *Client) CreateRole(params *CreateRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*CreateRoleCreated, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewCreateRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "createRole",
		Method:             "POST",
		PathPattern:        "/authz/roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &CreateRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*CreateRoleCreated)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for createRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
DeleteRole deletes role
*/
func (a *Client) DeleteRole(params *DeleteRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*DeleteRoleNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewDeleteRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "deleteRole",
		Method:             "DELETE",
		PathPattern:        "/authz/roles/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &DeleteRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*DeleteRoleNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for deleteRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRole gets a role
*/
func (a *Client) GetRole(params *GetRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRole",
		Method:             "GET",
		PathPattern:        "/authz/roles/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRoles gets all roles
*/
func (a *Client) GetRoles(params *GetRolesParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRolesParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRoles",
		Method:             "GET",
		PathPattern:        "/authz/roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRolesReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetRolesOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRoles: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRolesForUser gets roles assigned to user or a key
*/
func (a *Client) GetRolesForUser(params *GetRolesForUserParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesForUserOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRolesForUserParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRolesForUser",
		Method:             "GET",
		PathPattern:        "/authz/users/{id}/roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRolesForUserReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetRolesForUserOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRolesForUser: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetUsersForRole gets users or a keys assigned to role
*/
func (a *Client) GetUsersForRole(params *GetUsersForRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetUsersForRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetUsersForRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getUsersForRole",
		Method:             "GET",
		PathPattern:        "/authz/roles/{id}/users",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetUsersForRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetUsersForRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getUsersForRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
RemovedPermission removes permission from a role
*/
func (a *Client) RemovedPermission(params *RemovedPermissionParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RemovedPermissionCreated, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewRemovedPermissionParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "removedPermission",
		Method:             "POST",
		PathPattern:        "/authz/roles/remove-permission",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &RemovedPermissionReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*RemovedPermissionCreated)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for removedPermission: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
RevokeRole revokes a role from a user
*/
func (a *Client) RevokeRole(params *RevokeRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RevokeRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewRevokeRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "revokeRole",
		Method:             "POST",
		PathPattern:        "/authz/users/{id}/revoke",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &RevokeRoleReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*RevokeRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for revokeRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
