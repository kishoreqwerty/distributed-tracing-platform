// Package topology loads a synthetic service topology from YAML and
// generates traces that walk it, producing both the OTLP spans to emit and
// the ground truth of what was generated (see internal/groundtruth).
package topology

import (
	_ "embed"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

//go:embed default.yaml
var defaultYAML []byte

// Config is a synthetic system: a root service, the services in it, the
// call edges between them, and any incidents scheduled against it (see
// incident.go).
type Config struct {
	Root      string         `yaml:"root"`
	Services  []ServiceSpec  `yaml:"services"`
	Edges     []EdgeSpec     `yaml:"edges"`
	Incidents []IncidentSpec `yaml:"incidents"`

	byService map[string]ServiceSpec
	outgoing  map[string][]EdgeSpec

	// incidentWindows is Incidents resolved to absolute wall-clock time —
	// see ActivateIncidents. Empty (and every incident lookup a no-op)
	// until that's been called.
	incidentWindows []incidentWindow
}

// ServiceSpec describes one service's simulated latency.
type ServiceSpec struct {
	Name      string      `yaml:"name"`
	LatencyMS LatencySpec `yaml:"latency_ms"`
}

// LatencySpec is a normal distribution in milliseconds, sampled and
// clamped to a small positive minimum (see generate.go's minLatencyMS).
type LatencySpec struct {
	Mean   float64 `yaml:"mean"`
	Stddev float64 `yaml:"stddev"`
}

// EdgeSpec is one possible call from Caller to Callee, made with
// probability CallProbability each time Caller handles a request.
type EdgeSpec struct {
	Caller          string  `yaml:"caller"`
	Callee          string  `yaml:"callee"`
	CallProbability float64 `yaml:"call_probability"`
}

// Parse loads and validates a Config from YAML bytes.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse topology: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Load reads and parses a Config from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read topology file %s: %w", path, err)
	}
	return Parse(data)
}

// Default returns the topology shipped with loadgen: 6 services, one
// fan-out (checkout calling three downstream services), and one 4-level
// chain (frontend -> checkout -> shipping -> notifications).
func Default() (*Config, error) {
	return Parse(defaultYAML)
}

func (c *Config) validate() error {
	c.byService = make(map[string]ServiceSpec, len(c.Services))
	for _, s := range c.Services {
		if s.Name == "" {
			return fmt.Errorf("topology: service with empty name")
		}
		if _, dup := c.byService[s.Name]; dup {
			return fmt.Errorf("topology: duplicate service %q", s.Name)
		}
		if s.LatencyMS.Mean <= 0 {
			return fmt.Errorf("topology: service %q has non-positive latency_ms.mean", s.Name)
		}
		if s.LatencyMS.Stddev < 0 {
			return fmt.Errorf("topology: service %q has negative latency_ms.stddev", s.Name)
		}
		c.byService[s.Name] = s
	}

	if c.Root == "" {
		return fmt.Errorf("topology: root is required")
	}
	if _, ok := c.byService[c.Root]; !ok {
		return fmt.Errorf("topology: root %q is not a defined service", c.Root)
	}

	c.outgoing = make(map[string][]EdgeSpec, len(c.Services))
	for _, e := range c.Edges {
		if _, ok := c.byService[e.Caller]; !ok {
			return fmt.Errorf("topology: edge caller %q is not a defined service", e.Caller)
		}
		if _, ok := c.byService[e.Callee]; !ok {
			return fmt.Errorf("topology: edge callee %q is not a defined service", e.Callee)
		}
		if e.CallProbability <= 0 || e.CallProbability > 1 {
			return fmt.Errorf("topology: edge %s->%s call_probability must be in (0, 1], got %v", e.Caller, e.Callee, e.CallProbability)
		}
		c.outgoing[e.Caller] = append(c.outgoing[e.Caller], e)
	}

	for i := range c.Incidents {
		if c.Incidents[i].ID == "" {
			c.Incidents[i].ID = fmt.Sprintf("%s-%d", c.Incidents[i].Type, i)
		}
		if err := c.Incidents[i].validate(c.byService, c.outgoing); err != nil {
			return err
		}
	}

	return nil
}
