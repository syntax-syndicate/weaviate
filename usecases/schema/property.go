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

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// AddClassProperty it is upsert operation. it adds properties to a class and updates
// existing properties if the merge bool passed true.
func (h *Handler) AddClassProperty(ctx context.Context, principal *models.Principal,
	class *models.Class, merge bool, newProps ...*models.Property,
) (*models.Class, uint64, error) {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return nil, 0, err
	}

	if class == nil {
		return nil, 0, fmt.Errorf("class is nil: %w", ErrNotFound)
	}

	if len(newProps) == 0 {
		return nil, 0, nil
	}

	// validate new props
	for _, prop := range newProps {
		if prop.Name == "" {
			return nil, 0, fmt.Errorf("property must contain name")
		}
		prop.Name = schema.LowercaseFirstLetter(prop.Name)
		if prop.DataType == nil {
			return nil, 0, fmt.Errorf("property must contain dataType")
		}
	}

	if err := h.setNewPropDefaults(class, newProps...); err != nil {
		return nil, 0, err
	}

	existingNames := make(map[string]bool, len(class.Properties))
	if !merge {
		for _, prop := range class.Properties {
			existingNames[strings.ToLower(prop.Name)] = true
		}
	}

	if err := h.validateProperty(class, existingNames, false, newProps...); err != nil {
		return nil, 0, err
	}

	// TODO-RAFT use UpdateProperty() for adding/merging property when index idempotence exists
	// revisit when index idempotence exists and/or allowing merging properties on index.
	props := schema.DedupProperties(class.Properties, newProps)
	if len(props) == 0 {
		return class, 0, nil
	}

	migratePropertySettings(props...)

	class.Properties = clusterSchema.MergeProps(class.Properties, props)
	version, err := h.schemaManager.AddProperty(ctx, class.Class, props...)
	if err != nil {
		return nil, 0, err
	}
	return class, version, err
}

func (h *Handler) UpdateClassProperty(ctx context.Context, principal *models.Principal,
	class *models.Class, prop *models.Property,
) (*models.Class, uint64, error) {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return nil, 0, err
	}

	if class == nil {
		return nil, 0, fmt.Errorf("class is nil: %w", ErrNotFound)
	}

	if prop == nil {
		return nil, 0, fmt.Errorf("no property provided")
	}

	// Only supports setting indexRangeFilters for property with
	// indexFilterable already set.
	// Will fail for any other type of change.

	if prop.Name == "" {
		return nil, 0, fmt.Errorf("property name not provided")
	}

	var existingProp *models.Property
	for _, property := range class.Properties {
		if prop.Name == property.Name {
			existingProp = property
			break
		}
	}

	if existingProp == nil {
		return nil, 0, fmt.Errorf("property not found")
	}

	// validate
	if len(prop.DataType) > 0 {
		return nil, 0, fmt.Errorf("invalid property: data type provided")
	}
	if prop.Description != "" {
		return nil, 0, fmt.Errorf("invalid property: description provided")
	}
	if prop.IndexFilterable != nil {
		return nil, 0, fmt.Errorf("invalid property: index filterable provided")
	}
	if prop.IndexSearchable != nil {
		return nil, 0, fmt.Errorf("invalid property: index searchable provided")
	}
	if prop.IndexInverted != nil {
		return nil, 0, fmt.Errorf("invalid property: index inverted provided")
	}
	if prop.ModuleConfig != nil {
		return nil, 0, fmt.Errorf("invalid property: module config provided")
	}
	if len(prop.NestedProperties) > 0 {
		return nil, 0, fmt.Errorf("invalid property: nested properties provided")
	}
	if prop.Tokenization != "" {
		return nil, 0, fmt.Errorf("invalid property: tokenization provided")
	}

	switch dt, _ := schema.AsPrimitive(existingProp.DataType); dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeDate:
		// ok
	default:
		return nil, 0, fmt.Errorf("invalid property: unsupported data type %q", dt.String())
	}
	if !inverted.HasFilterableIndex(existingProp) {
		return nil, 0, fmt.Errorf("invalid property: filterable index needed as source")
	}

	if !(prop.IndexRangeFilters != nil && *prop.IndexRangeFilters) {
		return nil, 0, fmt.Errorf("index rangeable change to %v not allowed", *prop.IndexRangeFilters)
	}

	existingProp.IndexRangeFilters = prop.IndexRangeFilters

	version, err := h.schemaManager.AddProperty(ctx, class.Class, existingProp)
	if err != nil {
		return nil, 0, err
	}
	return class, version, err
}

// DeleteClassProperty from existing Schema
func (h *Handler) DeleteClassProperty(ctx context.Context, principal *models.Principal,
	class string, property string,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return fmt.Errorf("deleting a property is currently not supported, see " +
		"https://github.com/weaviate/weaviate/issues/973 for details.")
	// return h.deleteClassProperty(ctx, class, property, kind.Action)
}

func (h *Handler) setNewPropDefaults(class *models.Class, props ...*models.Property) error {
	setPropertyDefaults(props...)
	h.moduleConfig.SetSinglePropertyDefaults(class, props...)
	return nil
}

func (h *Handler) validatePropModuleConfig(class *models.Class, props ...*models.Property) error {
	for _, prop := range props {
		if prop.ModuleConfig == nil {
			continue
		}
		modconfig, ok := prop.ModuleConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%v property config invalid", prop.Name)
		}

		if !hasTargetVectors(class) {
			configuredVectorizers := make([]string, 0, len(modconfig))
			for modName := range modconfig {
				if err := h.vectorizerValidator.ValidateVectorizer(modName); err == nil {
					configuredVectorizers = append(configuredVectorizers, modName)
				}
			}
			if len(configuredVectorizers) > 1 {
				return fmt.Errorf("multiple vectorizers configured in property's %q moduleConfig: %v. class.vectorizer is set to %q",
					prop.Name, configuredVectorizers, class.Vectorizer)
			}

			vectorizerConfig, ok := modconfig[class.Vectorizer]
			if !ok {
				if class.Vectorizer == "none" {
					continue
				}
				return fmt.Errorf("%v vectorizer module not part of the property", class.Vectorizer)
			}
			_, ok = vectorizerConfig.(map[string]interface{})
			if !ok {
				return fmt.Errorf("vectorizer config for vectorizer %v, not of type map[string]interface{}", class.Vectorizer)
			}
			continue
		}

		// TODO reuse for multiple props?
		vectorizersSet := map[string]struct{}{}
		for _, cfg := range class.VectorConfig {
			if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
				for vectorizer := range vm {
					vectorizersSet[vectorizer] = struct{}{}
				}
			}
		}
		for vectorizer, cfg := range modconfig {
			if _, ok := vectorizersSet[vectorizer]; !ok {
				return fmt.Errorf("vectorizer %q not configured for any of target vectors", vectorizer)
			}
			if _, ok := cfg.(map[string]interface{}); !ok {
				return fmt.Errorf("vectorizer config for vectorizer %q not of type map[string]interface{}", vectorizer)
			}
		}
	}

	return nil
}
