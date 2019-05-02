package avro

import (
	"fmt"
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

func TestEqual(t *testing.T) {
	tests := []struct {
		A     Schema
		B     Schema
		Equal bool
	}{
		{
			A:     String,
			B:     String,
			Equal: true,
		},
		{
			A:     &Decimal{1, 3},
			B:     &Decimal{1, 3},
			Equal: true,
		},
		{
			A:     &Decimal{1, 2},
			B:     &Decimal{1, 3},
			Equal: false,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			equal := Equal(test.A, test.B)

			if equal && !test.Equal {
				t.Errorf("expected not equal")
			} else if !equal && test.Equal {
				t.Errorf("expected equal")
			}
		})
	}
}

func TestUnionContains(t *testing.T) {
	u := Union{
		Null,
		&Decimal{1, 2},
		String,
	}

	if !u.Contains(Null) {
		t.Errorf("expected null")
	}

	if !u.Contains(&Decimal{1, 2}) {
		t.Errorf("expected decimal(1, 2)")
	}

	if !u.Contains(String) {
		t.Errorf("expected string")
	}
}
