package jsonobj

import (
	"bytes"
	"testing"
)

// TestAppend verifies member addition, sorted order, empty maps, and key collisions.
func TestAppend(t *testing.T) {
	for _, tc := range []struct {
		name      string
		obj       string
		members   map[string]any
		want      string
		wantError bool
	}{
		{name: "members", obj: `{"core":1}`, members: map[string]any{"module": "value"}, want: `{"core":1,"module":"value"}`},
		{name: "empty object", obj: `{}`, members: map[string]any{"module": true}, want: `{"module":true}`},
		{name: "nil members", obj: `{"core":1}`, want: `{"core":1}`},
		{name: "empty members", obj: `{"core":1}`, members: map[string]any{}, want: `{"core":1}`},
		{name: "sorted members", obj: `{"core":1}`, members: map[string]any{"z": 2, "a": 3}, want: `{"core":1,"a":3,"z":2}`},
		{name: "collision", obj: `{"core":1}`, members: map[string]any{"core": 2}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := []byte(tc.obj)
			got, err := Append(original, tc.members)
			if (err != nil) != tc.wantError {
				t.Fatalf("Append error = %v, want error %v", err, tc.wantError)
			}
			if !tc.wantError && string(got) != tc.want {
				t.Fatalf("Append = %s, want %s", got, tc.want)
			}
			if !bytes.Equal(original, []byte(tc.obj)) {
				t.Fatalf("Append changed input: %s", original)
			}
		})
	}
}
