package module

import (
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Error codes that the core and module MCP tools share. A module may define
// its own codes; every code matches [a-z][a-z0-9_]*.
const (
	// CodeUnknownSerial reports a serial that is not configured.
	CodeUnknownSerial = "unknown_serial"
	// CodeCommandFailed reports a command that the backend refused or
	// could not apply.
	CodeCommandFailed = "command_failed"
)

// ToolError is the stable, machine-readable operational outcome carried in
// the error member of every MCP tool output. Codes are a closed set per
// tool; the message is human context and may change.
type ToolError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error returns the message so a ToolError can travel as a Go error.
func (e *ToolError) Error() string { return e.Message }

// ReadOnly builds the annotations of a tool that only reads cached state.
func ReadOnly(title string) *mcp.ToolAnnotations {
	no := false
	return &mcp.ToolAnnotations{
		Title:           title,
		ReadOnlyHint:    true,
		DestructiveHint: &no,
		IdempotentHint:  true,
		OpenWorldHint:   &no,
	}
}

// Command builds the annotations of a tool that changes printer or module
// state.
func Command(title string, destructive, idempotent bool) *mcp.ToolAnnotations {
	no := false
	return &mcp.ToolAnnotations{
		Title:           title,
		ReadOnlyHint:    false,
		DestructiveHint: &destructive,
		IdempotentHint:  idempotent,
		OpenWorldHint:   &no,
	}
}

// SerialInput builds a closed object input schema with a required,
// non-empty "serial" property plus extra properties. required lists the
// extra properties that are required, after "serial".
func SerialInput(extra map[string]*jsonschema.Schema, required ...string) *jsonschema.Schema {
	minLen := 1
	props := map[string]*jsonschema.Schema{
		"serial": {Type: "string", Description: "Printer serial number.", MinLength: &minLen},
	}
	for name, schema := range extra {
		props[name] = schema
	}
	return &jsonschema.Schema{
		Type:                 "object",
		Properties:           props,
		Required:             append([]string{"serial"}, required...),
		AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
	}
}
