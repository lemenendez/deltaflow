package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

const delimiter = "."

type LoadOptions struct {
	Overrides map[string]any
}

type Config struct {
	Store     StoreConfig      `koanf:"store" validate:"required"`
	Workers   WorkersConfig    `koanf:"workers" validate:"required"`
	Pipelines []PipelineConfig `koanf:"pipelines" validate:"required,min=1,dive"`
}

type StoreConfig struct {
	Type string `koanf:"type" validate:"required"`
	DSN  string `koanf:"dsn" validate:"required"`
}

type WorkersConfig struct {
	Concurrency int    `koanf:"concurrency" validate:"required,gt=0"`
	LeaseTTL    string `koanf:"lease_ttl" validate:"required"`
	PullSize    *int   `koanf:"pull_size" validate:"omitempty,gt=0"`
	MaxAttempts *int   `koanf:"max_attempts" validate:"omitempty,gt=0"`
}

type PipelineConfig struct {
	Name      string          `koanf:"name" validate:"required"`
	SyncID    string          `koanf:"sync_id" validate:"required"`
	Source    SourceConfig    `koanf:"source" validate:"required"`
	Projector ProjectorConfig `koanf:"projector" validate:"required"`
	Target    TargetConfig    `koanf:"target" validate:"required"`
	Applier   ApplierConfig   `koanf:"applier" validate:"required"`
}

type SourceConfig struct {
	Type           string `koanf:"type" validate:"required"`
	ProjectionType string `koanf:"projection_type" validate:"required"`
}

type ProjectorConfig struct {
	Name string `koanf:"name" validate:"required"`
}

type TargetConfig struct {
	Type  string `koanf:"type" validate:"required"`
	Index string `koanf:"index" validate:"required"`
}

type ApplierConfig struct {
	Mode string `koanf:"mode" validate:"required"`
}

type ValidationError struct {
	Issues []string
}

func (e ValidationError) Error() string {
	if len(e.Issues) == 0 {
		return "invalid config"
	}
	return "invalid config:\n- " + strings.Join(e.Issues, "\n- ")
}

func LoadFile(path string, opts LoadOptions) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("config path is required")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	k := koanf.New(delimiter)
	expanded := os.ExpandEnv(string(raw))
	if err := k.Load(rawbytes.Provider([]byte(expanded)), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if len(opts.Overrides) > 0 {
		if err := k.Load(confmap.Provider(opts.Overrides, delimiter), nil); err != nil {
			return nil, err
		}
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("decode config %q: %w", path, err)
	}

	if k.Exists("version") {
		return nil, ValidationError{Issues: []string{"version is not supported yet; remove the version field"}}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c Config) Validate() error {
	var issues []string

	v := validator.New(validator.WithRequiredStructEnabled())
	if err := v.Struct(c); err != nil {
		issues = append(issues, validationIssues(err)...)
	}

	if c.Store.Type != "" && c.Store.Type != "postgres" {
		issues = append(issues, "store.type must be postgres")
	}

	if c.Workers.LeaseTTL != "" {
		if _, err := parsePositiveDuration(c.Workers.LeaseTTL); err != nil {
			issues = append(issues, fmt.Sprintf("workers.lease_ttl %s", err))
		}
	}

	for i, p := range c.Pipelines {
		prefix := fmt.Sprintf("pipelines[%d]", i)
		if p.Source.Type != "" && p.Source.Type != "postgres-outbox" {
			issues = append(issues, fmt.Sprintf("%s.source.type must be postgres-outbox", prefix))
		}
		if p.Target.Type != "" && p.Target.Type != "elasticsearch" {
			issues = append(issues, fmt.Sprintf("%s.target.type must be elasticsearch", prefix))
		}
		if p.Applier.Mode != "" && p.Applier.Mode != "upsert" {
			issues = append(issues, fmt.Sprintf("%s.applier.mode must be upsert", prefix))
		}
	}

	if len(issues) > 0 {
		sort.Strings(issues)
		return ValidationError{Issues: dedupe(issues)}
	}

	return nil
}

func (w WorkersConfig) LeaseTTLDuration() (time.Duration, error) {
	return parsePositiveDuration(w.LeaseTTL)
}

func parsePositiveDuration(value string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("must be a valid duration: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return d, nil
}

func validationIssues(err error) []string {
	var validationErrs validator.ValidationErrors
	if !errors.As(err, &validationErrs) {
		return []string{err.Error()}
	}

	issues := make([]string, 0, len(validationErrs))
	for _, fieldErr := range validationErrs {
		field := configFieldName(fieldErr.Namespace())
		switch fieldErr.Tag() {
		case "required":
			issues = append(issues, fmt.Sprintf("%s is required", field))
		case "min":
			issues = append(issues, fmt.Sprintf("%s must contain at least %s item(s)", field, fieldErr.Param()))
		case "gt":
			issues = append(issues, fmt.Sprintf("%s must be greater than %s", field, fieldErr.Param()))
		default:
			issues = append(issues, fmt.Sprintf("%s failed %s validation", field, fieldErr.Tag()))
		}
	}
	return issues
}

func configFieldName(namespace string) string {
	segmentNames := map[string]string{
		"Store":          "store",
		"Workers":        "workers",
		"Pipelines":      "pipelines",
		"Source":         "source",
		"Projector":      "projector",
		"Target":         "target",
		"Applier":        "applier",
		"Type":           "type",
		"DSN":            "dsn",
		"Concurrency":    "concurrency",
		"LeaseTTL":       "lease_ttl",
		"PullSize":       "pull_size",
		"MaxAttempts":    "max_attempts",
		"Name":           "name",
		"SyncID":         "sync_id",
		"ProjectionType": "projection_type",
		"Index":          "index",
		"Mode":           "mode",
	}

	parts := strings.Split(namespace, ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "Config" {
			continue
		}

		name, suffix, _ := strings.Cut(part, "[")
		mapped, ok := segmentNames[name]
		if !ok {
			mapped = name
		}
		if suffix != "" {
			mapped += "[" + suffix
		}
		out = append(out, mapped)
	}

	return strings.Join(out, ".")
}

func dedupe(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := values[:0]
	var prev string
	for _, value := range values {
		if value == prev {
			continue
		}
		out = append(out, value)
		prev = value
	}
	return out
}
