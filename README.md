# go-avro

[![GoDoc](https://godoc.org/github.com/arcus/go-avro?status.svg)](https://godoc.org/github.com/arcus/go-avro)

Go library for working with Avro schema and data.

## Usage

Build a record-based Avro schema.

```go
sexEnum := &avro.Enum{
  Name: "sex",
  Symbols: []string{
    "Male",
    "Female",
    "Unknown",
  },
}

participantSchema := &avro.Record{
  Name: "participant",

  Fields: []*avro.Field{
    {
      Name: "id",
      Type: String,
    },
    {
      Name: "birth_date",
      Type: avro.Union{
        avro.Null,
        avro.Date,
      },
    },
    {
      Name: "sex",
      Type: avro.Union{
        avro.Null,
        sexEnum,
      },
    },
  },
}
```

Marshal schema to JSON-encoded representation.

```go
bytes, err := avro.Marshal(participantSchema)
```

Unmarshal encoded schema into its native type.

```go
var participantSchema Record
avro.UnmarshalSchema(bytes, &participantSchema)
```

## Integration with the [LinkedIn GoAvro](https://github.com/linked/goavro) library

```go
// SchemaToCodec returns a new goavro.Codec from the provided avro.Schema.
func SchemaToCodec(s avro.Schema) (*goavro.Codec, error) {
	b, err := avro.Marshal(s)
	if err != nil {
		return nil, err
	}
	return goavro.NewCodec(string(b))
}

// CodecToSchema derives an avro.Schema from the provided goavro.Codec.
func CodecToSchema(c *goavro.Codec) (avro.Schema, error) {
	return avro.Unmarshal([]byte(c.Schema()))
}
```
