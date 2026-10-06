package module

import (
	"fmt"
	"net/http"
	"strings"
)

// Route is one HTTP handler a module serves on the shared listener.
type Route struct {
	// Pattern is a net/http ServeMux pattern that starts with a method.
	// For example, "PUT /detection/{serial}".
	Pattern string
	Handler http.Handler
}

// Check rejects an empty or duplicate Name and route patterns without a method.
func Check(mods []Module) error {
	names := make(map[string]bool, len(mods))
	for _, mod := range mods {
		if mod.Name == "" {
			return fmt.Errorf("module name is empty")
		}
		if names[mod.Name] {
			return fmt.Errorf("duplicate module name %q", mod.Name)
		}
		names[mod.Name] = true
		for _, route := range mod.Routes {
			// A ServeMux pattern is "[METHOD ][HOST]/PATH"; a method-less
			// pattern has one field, so Mount could not protect its writes.
			if fields := strings.Fields(route.Pattern); len(fields) != 2 || strings.Contains(fields[0], "/") {
				return fmt.Errorf("module %q route %q must start with an HTTP method", mod.Name, route.Pattern)
			}
		}
	}
	return nil
}

// Mount registers every module route on mux.
// Routes whose method is not GET or HEAD share cross-origin protection.
func Mount(mux *http.ServeMux, mods []Module) {
	protection := http.NewCrossOriginProtection()
	for _, mod := range mods {
		for _, route := range mod.Routes {
			method := strings.Fields(route.Pattern)[0]
			handler := route.Handler
			if method != http.MethodGet && method != http.MethodHead {
				handler = protection.Handler(handler)
			}
			mux.Handle(route.Pattern, handler)
		}
	}
}
