package config

import (
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"os"
)

type Configuration struct {
	OldRedis             Redis `yaml:"old_redis"`
	ConcurrentWorkers    uint  `yaml:"concurrent_workers"`
	NewRedis             Redis `yaml:"new_redis"`
	Databases            []int `yaml:"migration_databases"`
	ClearBeforeMigration bool  `yaml:"clear_before_migration"`
}

type Redis struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
}

// ParseConfig is to parse YAML configuration file
func ParseConfig(configFile string) Configuration {
	var configContent Configuration
	yamlFile, err := os.ReadFile(configFile)
	if err != nil {
		logrus.Errorf("Error while reading file %v", err)
	}
	err = yaml.Unmarshal(yamlFile, &configContent)
	if err != nil {
		logrus.Errorf("Error in parsing file to yaml content %v", err)
	}
	return configContent
}
