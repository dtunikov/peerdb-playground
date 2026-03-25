package config

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	//go:embed config.schema.json
	JSONSchema []byte
)

func ValidateConfig(yamlData []byte) error {
	var v any
	if err := yaml.Unmarshal(yamlData, &v); err != nil {
		log.Fatal(err)
	}

	c := jsonschema.NewCompiler()
	c.AssertFormat()
	c.AssertVocabs()
	c.RegisterFormat(&jsonschema.Format{
		Name:     "go-duration",
		Validate: isGoDuration,
	})
	c.RegisterFormat(&jsonschema.Format{
		Name:     "url",
		Validate: isURL,
	})

	schema, err := jsonschema.UnmarshalJSON(bytes.NewReader(JSONSchema))
	if err != nil {
		return fmt.Errorf("could not unmarshal json schema: %w", err)
	}

	schemaID := "schema.json"
	c.AddResource(schemaID, schema)

	sch, err := c.Compile(schemaID)
	if err != nil {
		return fmt.Errorf("could not compile json schema: %w", err)
	}

	err = sch.Validate(v)
	if err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	return nil
}

// isGoDuration is the validation function for validating if the current field's value is a valid Go duration.
func isGoDuration(s any) error {
	val, ok := s.(string)
	if !ok {
		return errors.New("invalid duration")
	}
	_, err := time.ParseDuration(val)
	return err
}

// isFileURL is the helper function for validating if the `path` valid file URL as per RFC8089
func isFileURL(path string) bool {
	if !strings.HasPrefix(path, "file:/") {
		return false
	}
	_, err := url.ParseRequestURI(path)
	return err == nil
}

// isURL is the validation function for validating if the current field's value is a valid URL.
func isURL(a any) error {
	val, ok := a.(string)
	if !ok {
		return errors.New("invalid URL")
	}
	s := strings.ToLower(val)

	if len(s) == 0 {
		return errors.New("invalid URL")
	}

	if isFileURL(s) {
		return errors.New("invalid URL")
	}

	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" {
		return errors.New("invalid URL")
	}

	if u.Host == "" && u.Fragment == "" && u.Opaque == "" {
		return errors.New("invalid URL")
	}

	return nil
}
