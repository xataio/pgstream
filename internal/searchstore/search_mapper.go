// SPDX-License-Identifier: Apache-2.0

package searchstore

type Mapper interface {
	GetDefaultIndexSettings() map[string]any
	FieldMapping(*Field) (map[string]any, error)
}

type Field struct {
	SearchType Type
	IsArray    bool
	Metadata   Metadata
}

type Metadata struct {
	VectorDimension int
}

type Type uint

const (
	IntegerType Type = iota
	FloatType
	BoolType
	StringType
	DateTimeTZType
	DateTimeType
	DateType
	TimeType
	JSONType
	TextType
	PGVectorType
)
