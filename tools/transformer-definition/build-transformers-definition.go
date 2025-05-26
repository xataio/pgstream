// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/xataio/pgstream/pkg/transformers"
	"github.com/xataio/pgstream/pkg/transformers/builder"
)

type Result struct {
	Name         string        `json:"name"`
	Transformers []Transformer `json:"transformers"`
}

type Transformer struct {
	Name           string      `json:"name"`
	SupportedTypes []string    `json:"supported_types"`
	Parameters     []Parameter `json:"parameters"`
}

type Parameter struct {
	Name          string `json:"name"`
	SupportedType string `json:"supported_type"`
	Default       any    `json:"default"`
	Dynamic       bool   `json:"dynamic"`
	Required      bool   `json:"required"`
	Values        []any  `json:"values,omitempty"`
}

func main() {
	log.Println("Generating Transformers JSON schema...")

	result := Result{
		Name:         "transformers",
		Transformers: extractTransformers(builder.TransformersMap),
	}

	if err := writeJSONToFile("transformers-definition.json", result); err != nil {
		log.Fatalf("failed to write JSON to file: %v", err)
	}

	log.Println("Transformers JSON schema generated successfully")
}

func extractTransformers(transformersMap map[transformers.TransformerType]builder.TransformerDefinition) []Transformer {
	// Sort the keys to ensure consistent ordering
	keys := make([]string, 0, len(transformersMap))
	for trName := range transformersMap {
		keys = append(keys, string(trName))
	}
	sort.Strings(keys)

	transformersList := make([]Transformer, 0, len(transformersMap))
	for _, trName := range keys {
		transformer := transformersMap[transformers.TransformerType(trName)]
		transformersList = append(transformersList, Transformer{
			Name:           trName,
			SupportedTypes: extractSupportedTypes(transformer.SupportedTypes),
			Parameters:     extractParameters(transformer.Parameters),
		})
	}

	return transformersList
}

func extractSupportedTypes(types []transformers.SupportedDataType) []string {
	supportedTypes := make([]string, 0, len(types))
	for _, st := range types {
		supportedTypes = append(supportedTypes, string(st))
	}
	return supportedTypes
}

func extractParameters(params []transformers.Parameter) []Parameter {
	parameters := make([]Parameter, 0, len(params))
	for _, param := range params {
		parameters = append(parameters, Parameter{
			Name:          param.Name,
			SupportedType: param.SupportedType,
			Default:       param.Default,
			Dynamic:       param.Dynamic,
			Required:      param.Required,
			Values:        param.Values,
		})
	}
	return parameters
}

func writeJSONToFile(filename string, data any) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}
	return nil
}
