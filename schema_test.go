package avro

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSchema(t *testing.T) {
	r1 := &Record{
		Name: "Record",
		Fields: []*Field{
			{
				Name: "arcus_id",
				Type: String,
			},
			{
				Name: "dob",
				Type: Union{
					Null,
					Date,
				},
			},
			{
				Name: "ethnicity",
				Type: Union{
					Null,
					&Enum{
						Name: "sex",
						Symbols: []string{
							"Male",
							"Female",
							"Unknown",
						},
					},
				},
			},
		},
	}

	// Encode.
	b, err := Marshal(r1)
	if err != nil {
		t.Fatal(err)
	}

	// Decode.
	var r2 Record
	if err := UnmarshalSchema(b, &r2); err != nil {
		t.Fatal(err)
	}

	// Compare to ensure schema unmarshaling worked.
	if diff := cmp.Diff(r1, &r2); diff != "" {
		t.Errorf("(-want +got)\n%s", diff)
	}
}
