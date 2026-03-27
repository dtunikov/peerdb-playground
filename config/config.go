package config

import (
	"fmt"
	"os"

	_ "embed"

	"github.com/caarlos0/env/v11"
	"github.com/goccy/go-yaml"
)

type Log struct {
	Level string `yaml:"level" env:"LOG_LEVEL" envDefault:"info"`
}

type Server struct {
	Port int `yaml:"port" env:"SERVER_PORT" envDefault:"8080"`
}

type PostgresConfig struct {
	Url string `yaml:"url" env:"POSTGRES_URL"`
}

type TemporalConfig struct {
	HostPort string `yaml:"host_port" env:"TEMPORAL_HOST_PORT" envDefault:"localhost:7233"`
}

type Config struct {
	Log           Log            `yaml:"log"`
	Server        Server         `yaml:"server"`
	Database      PostgresConfig `yaml:"database"`
	Temporal      TemporalConfig `yaml:"temporal"`
	EncryptionKey string         `yaml:"encryption_key" env:"ENCRYPTION_KEY"`
}

func LoadConfig(configFilePath string) (*Config, error) {
	cfg := Config{}

	configFileBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not read custom config file %s: %w", configFilePath, err)
	}

	configYamlData := os.ExpandEnv(string(configFileBytes))
	if err := yaml.Unmarshal([]byte(configYamlData), &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal router config: %w", err)
	}

	// Parse env vars after YAML so environment overrides file values
	err = env.Parse(&cfg)
	if err != nil {
		return nil, err
	}

	// Validate the config against the JSON schema
	configFileBytes = []byte(configYamlData)
	err = ValidateConfig(configFileBytes)
	if err != nil {
		return nil, fmt.Errorf("router config validation error: %w", err)
	}

	return &cfg, nil
}
