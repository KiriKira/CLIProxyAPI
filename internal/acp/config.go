package acp

import (
	"context"
	"encoding/json"
	"fmt"
)

// SessionConfigSelectOption is one selectable value of a select-type
// session configuration option (e.g. one model).
type SessionConfigSelectOption struct {
	Name        string `json:"name"`
	Value       string `json:"value"`
	Description string `json:"description,omitempty"`
}

// SessionConfigOption is one entry of the configOptions array returned by
// session/new and session/set_config_option. The ACP schema defines it as
// a union: select options carry currentValue plus flat or grouped option
// lists, boolean options carry a value. Unknown shapes decode to their
// identity fields (ID/Type) with empty values rather than failing.
type SessionConfigOption struct {
	ID          string
	Type        string // "select", "boolean", or a future kind
	Category    string // "model", "mode", "thought_level", or custom
	Name        string
	Description string
	// Select-type fields.
	CurrentValue string
	Options      []SessionConfigSelectOption // flattened across groups
	// Boolean-type fields.
	BoolValue    bool
	HasBoolValue bool
}

// UnmarshalJSON decodes the select/boolean union, flattening grouped
// option lists (elements carrying a nested "options" array) into Options.
func (o *SessionConfigOption) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	for key, ptr := range map[string]*string{
		"id": &o.ID, "type": &o.Type, "category": &o.Category,
		"name": &o.Name, "description": &o.Description,
	} {
		if v, ok := raw[key]; ok {
			if err := json.Unmarshal(v, ptr); err != nil {
				return fmt.Errorf("acp: config option field %q: %w", key, err)
			}
		}
	}
	switch o.Type {
	case "select":
		if v, ok := raw["currentValue"]; ok {
			if err := json.Unmarshal(v, &o.CurrentValue); err != nil {
				return fmt.Errorf("acp: config option %q currentValue: %w", o.ID, err)
			}
		}
		if v, ok := raw["options"]; ok {
			opts, err := flattenSelectOptions(v)
			if err != nil {
				return fmt.Errorf("acp: config option %q options: %w", o.ID, err)
			}
			o.Options = opts
		}
	case "boolean":
		if v, ok := raw["value"]; ok {
			var bv bool
			if err := json.Unmarshal(v, &bv); err != nil {
				return fmt.Errorf("acp: boolean config option %q has non-boolean value", o.ID)
			}
			o.BoolValue = bv
			o.HasBoolValue = true
		}
	}
	return nil
}

// flattenSelectOptions decodes a select option list that may mix flat
// entries ({name, value}) with grouped entries ({..., options: [...]}).
func flattenSelectOptions(raw json.RawMessage) ([]SessionConfigSelectOption, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	var out []SessionConfigSelectOption
	for _, e := range elems {
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(e, &probe); err != nil {
			return nil, err
		}
		if nested, ok := probe["options"]; ok {
			var group []SessionConfigSelectOption
			if err := json.Unmarshal(nested, &group); err != nil {
				return nil, err
			}
			out = append(out, group...)
			continue
		}
		var opt SessionConfigSelectOption
		if err := json.Unmarshal(e, &opt); err != nil {
			return nil, err
		}
		out = append(out, opt)
	}
	return out, nil
}

// ModelOptionValues returns the flattened values of the id=="model"
// select option, or nil when the agent offers no model selector.
func ModelOptionValues(opts []SessionConfigOption) []string {
	for _, o := range opts {
		if o.ID == "model" && o.Type == "select" {
			out := make([]string, 0, len(o.Options))
			for _, e := range o.Options {
				out = append(out, e.Value)
			}
			return out
		}
	}
	return nil
}

// CurrentModel returns the agent's current model selection, or "" when
// the agent offers no model selector.
func CurrentModel(opts []SessionConfigOption) string {
	for _, o := range opts {
		if o.ID == "model" && o.Type == "select" {
			return o.CurrentValue
		}
	}
	return ""
}

// ResolveModel picks the model for a turn: an explicitly requested model
// is used as-is when the account offers it; otherwise the configured
// default is used when offered; otherwise the agent's current selection
// stands. It returns "" when the agent offers no model selector.
func ResolveModel(opts []SessionConfigOption, requested, def string) string {
	values := ModelOptionValues(opts)
	offered := func(v string) bool {
		for _, o := range values {
			if o == v {
				return true
			}
		}
		return false
	}
	if requested != "" && offered(requested) {
		return requested
	}
	if def != "" && offered(def) {
		return def
	}
	return CurrentModel(opts)
}

// setConfigOptionRequest selects a session configuration value, e.g.
// {configId: "model", value: "<model-id>"}. Value may be a string or bool.
type setConfigOptionRequest struct {
	SessionID string `json:"sessionId"`
	ConfigID  string `json:"configId"`
	Value     any    `json:"value"`
}

// setConfigOptionResponse carries the refreshed configOptions snapshot.
type setConfigOptionResponse struct {
	ConfigOptions []SessionConfigOption `json:"configOptions"`
}

// SetConfigOption updates one session configuration option and refreshes
// the locally cached configOptions snapshot from the agent response. It
// returns the refreshed snapshot.
func (c *Client) SetConfigOption(ctx context.Context, sessionID, configID string, value any) ([]SessionConfigOption, error) {
	raw, err := c.call(ctx, "session/set_config_option", setConfigOptionRequest{
		SessionID: sessionID,
		ConfigID:  configID,
		Value:     value,
	})
	if err != nil {
		return nil, err
	}
	var resp setConfigOptionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("acp: decode session/set_config_option response: %w", err)
	}
	c.storeConfigOptions(sessionID, resp.ConfigOptions)
	return c.ConfigOptions(sessionID), nil
}

// storeConfigOptions replaces the cached snapshot for a session.
func (c *Client) storeConfigOptions(sessionID string, opts []SessionConfigOption) {
	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	if c.sessCfg == nil {
		c.sessCfg = make(map[string][]SessionConfigOption)
	}
	c.sessCfg[sessionID] = append([]SessionConfigOption(nil), opts...)
}

// ConfigOptions returns a copy of the last known configOptions snapshot
// for a session, or nil when the session is unknown.
func (c *Client) ConfigOptions(sessionID string) []SessionConfigOption {
	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	return append([]SessionConfigOption(nil), c.sessCfg[sessionID]...)
}
