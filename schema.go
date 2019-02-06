package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const (
	// Primitive types.
	// https://avro.apache.org/docs/current/spec.html#schema_primitive
	Null    Primitive = "null"
	Boolean Primitive = "boolean"
	Int     Primitive = "int"
	Long    Primitive = "long"
	Float   Primitive = "float"
	Double  Primitive = "double"
	Bytes   Primitive = "bytes"
	String  Primitive = "string"
)

var (
	// Logical types.
	// https://avro.apache.org/docs/current/spec.html#Logical+Types
	Date            Schema = &date{}
	TimeMillis      Schema = &timeMillis{}
	TimeMicros      Schema = &timeMicros{}
	TimestampMillis Schema = &timestampMillis{}
	TimestampMicros Schema = &timestampMicros{}
	Duration        Schema = &duration{}
)

// Marshal marshals a schema to its binary representation which is encoded JSON.
func Marshal(s Schema) ([]byte, error) {
	return json.Marshal(s)
}

// UnmarshalSchema unmarshals an encoded schema into a known schema type.
func UnmarshalSchema(b []byte, s Schema) error {
	return json.Unmarshal(b, s)
}

// Unmarshal unmarshals an encoded schema into a schema value.
func Unmarshal(b []byte) (Schema, error) {
	b = bytes.TrimSpace(b)

	// Nothing to do.
	if len(b) == 0 {
		return nil, nil
	}

	// Decode a schema value into its native type.
	switch b[0] {
	// String-based type, so this is a primitive.
	case '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}

		// This does not imply this is a valid primitive type.
		return Primitive(s), nil

		// Square bracket implies a union.
	case '[':
		var u Union
		if err := json.Unmarshal(b, &u); err != nil {
			return nil, err
		}

		return u, nil

		// Curly brace implies a complex or logical type.
	case '{':
		// Decode just enough to determine the type.
		type structType struct {
			Type        string `json:"type"`
			LogicalType string `json:"logicalType"`
		}

		var s structType
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}

		var x Schema

		// Check for logical types.
		if s.LogicalType != "" {
			switch s.LogicalType {
			case "date":
				x = Date
			case "time-millis":
				x = TimeMillis
			case "time-micros":
				x = TimeMicros
			case "timestamp-millis":
				x = TimestampMillis
			case "timestamp-micros":
				x = TimestampMicros
			case "duration":
				x = Duration
			default:
				return nil, fmt.Errorf("avroschema: unknown logical type %v", s.LogicalType)
			}

			return x, nil
		}

		// Check for complex type.

		switch s.Type {
		case "record":
			x = &Record{}
		case "enum":
			x = &Enum{}
		case "array":
			x = &Array{}
		case "map":
			x = &Map{}
		case "fixed":
			x = &Fixed{}
		default:
			return nil, fmt.Errorf("avroschema: unknown complex type %v", s.Type)
		}

		if err := json.Unmarshal(b, x); err != nil {
			return nil, err
		}

		return x, nil
	}

	return nil, fmt.Errorf("avroschema: could not unmarshal %v as Schema", string(b))
}

// Schema models an Avro schema definition.
// https://avro.apache.org/docs/current/spec.html#schemas
type Schema interface {
	// Type returns the type name as defined by the Avro spec.
	Type() string
}

// Primitive models an Avro primitive type.
type Primitive string

// Type satisfies the Schema interface for primitive types.
func (p Primitive) Type() string {
	return string(p)
}

type Field struct {
	Name    string      `json:"name"`
	Type    Schema      `json:"type"`
	Doc     string      `json:"doc,omitempty"`
	Default interface{} `json:"default,omitempty"`
	Aliases []string    `json:"aliases,omitempty"`
	Order   string      `json:"order,omitempty"`
}

func (f *Field) UnmarshalJSON(b []byte) error {
	type proxy struct {
		Name    string          `json:"name"`
		Type    json.RawMessage `json:"type"`
		Doc     string          `json:"doc,omitempty"`
		Default interface{}     `json:"default,omitempty"`
		Aliases []string        `json:"aliases,omitempty"`
		Order   string          `json:"order,omitempty"`
	}

	var p proxy
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	f.Name = p.Name
	f.Doc = p.Doc
	f.Default = p.Default
	f.Aliases = p.Aliases
	f.Order = p.Order

	t, err := Unmarshal(p.Type)
	if err != nil {
		return err
	}
	f.Type = t

	return nil
}

type Record struct {
	Name      string
	Namespace string
	Doc       string
	Aliases   []string
	Fields    []*Field
}

func (r *Record) Type() string {
	return "record"
}

func (r *Record) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"type":   "record",
		"name":   r.Name,
		"fields": r.Fields,
	}

	if r.Namespace != "" {
		m["namespace"] = r.Namespace
	}

	if r.Doc != "" {
		m["doc"] = r.Doc
	}

	if len(r.Aliases) > 0 {
		m["aliases"] = r.Aliases
	}

	return json.Marshal(m)
}

type Enum struct {
	Name      string
	Namespace string
	Doc       string
	Aliases   []string
	Symbols   []string
}

func (e *Enum) Type() string {
	return "enum"
}

func (e *Enum) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"type":    "enum",
		"name":    e.Name,
		"symbols": e.Symbols,
	}

	if e.Namespace != "" {
		m["namespace"] = e.Namespace
	}

	if e.Doc != "" {
		m["doc"] = e.Doc
	}

	if len(e.Aliases) > 0 {
		m["aliases"] = e.Aliases
	}

	return json.Marshal(m)
}

type Array struct {
	Items Schema
}

func (a *Array) Type() string {
	return "array"
}

func (a *Array) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":  "array",
		"items": a.Items,
	})
}

func (a *Array) UnmarshalJSON(b []byte) error {
	type proxy struct {
		Type  string
		Items json.RawMessage
	}

	var p proxy
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	t, err := Unmarshal(p.Items)
	if err != nil {
		return err
	}

	a.Items = t
	return nil
}

type Map struct {
	Values Schema
}

func (m *Map) Type() string {
	return "map"
}

func (m *Map) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":   "map",
		"values": m.Values,
	})
}

func (m *Map) UnmarshalJSON(b []byte) error {
	type proxy struct {
		Type   string
		Values json.RawMessage
	}

	var p proxy
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	t, err := Unmarshal(p.Values)
	if err != nil {
		return err
	}

	m.Values = t
	return nil
}

type Union []Schema

func (u Union) Type() string {
	return "union"
}

func (u *Union) UnmarshalJSON(b []byte) error {
	var p []json.RawMessage
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	x := make(Union, len(p))
	for i, e := range p {
		t, err := Unmarshal(e)
		if err != nil {
			return err
		}
		x[i] = t
	}

	*u = x
	return nil
}

type Fixed struct {
	Name      string
	Size      int
	Namespace string
	Aliases   []string
}

func (f *Fixed) Type() string {
	return "fixed"
}

func (f *Fixed) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"type": "fixed",
		"name": f.Name,
	}

	if f.Namespace != "" {
		m["namespace"] = f.Namespace
	}

	if len(f.Aliases) > 0 {
		m["aliases"] = f.Aliases
	}

	return json.Marshal(m)
}

type Decimal struct {
	Precision int
	Scale     int
}

func (d *Decimal) Type() string {
	return "decimal"
}

func (d *Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "bytes",
		"logicalType": "decimal",
		"precision":   d.Precision,
		"scale":       d.Scale,
	})
}

type date struct{}

func (d *date) Type() string {
	return "date"
}

func (d *date) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "int",
		"logicalType": "date",
	})
}

type timeMillis struct{}

func (t *timeMillis) Type() string {
	return "time-millis"
}

func (t *timeMillis) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "int",
		"logicalType": "time-millis",
	})
}

type timeMicros struct{}

func (t *timeMicros) Type() string {
	return "time-micros"
}

func (t *timeMicros) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "long",
		"logicalType": "time-micros",
	})
}

type timestampMillis struct{}

func (t *timestampMillis) Type() string {
	return "timestamp-millis"
}

func (t *timestampMillis) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "long",
		"logicalType": "timestamp-millis",
	})
}

type timestampMicros struct{}

func (t *timestampMicros) Type() string {
	return "timestamp-micros"
}

func (t *timestampMicros) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "long",
		"logicalType": "timestamp-micros",
	})
}

type duration struct{}

func (d *duration) Type() string {
	return "duration"
}

func (d *duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":        "fixed",
		"logicalType": "duration",
		"size":        12,
	})
}
