// Package jsonobj adds module-owned members to marshaled JSON objects.
package jsonobj

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Append returns obj with members added after existing members in sorted key order.
// Empty members return obj. A member that collides with an existing key is rejected.
func Append(obj []byte, members map[string]any) ([]byte, error) {
	if len(members) == 0 {
		return obj, nil
	}
	var existing map[string]json.RawMessage
	if err := json.Unmarshal(obj, &existing); err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(obj)
	if len(trimmed) < 2 || trimmed[0] != '{' {
		return nil, fmt.Errorf("JSON value is not an object")
	}
	for key := range members {
		if _, ok := existing[key]; ok {
			return nil, fmt.Errorf("JSON member %q already exists", key)
		}
	}
	extra, err := json.Marshal(members)
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), trimmed[:len(trimmed)-1]...)
	if len(existing) != 0 {
		out = append(out, ',')
	}
	out = append(out, extra[1:]...)
	return out, nil
}
