package config

import (
	"log"

	"github.com/spf13/viper"
)

type Configuration struct {
	PORT             string
	KAFKAURL         string
	KAFKASERVER      string
	KAFKATOPIC       string
	KAFKAGROUPID     string
	KAFKARESETPOLICY string
	MONGODBURL       string
	MONGODB          string
	MONGOCOLLECTION  string
	MONGOTIMEOUT     int
}

// TODO: add replacer . by _
func LoadViperConfig() *Configuration {
	viper.AddConfigPath("./pkg/config")
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	// viper.EnvKeyReplacer(strings.NewReplacer(".", "_"))
	var configuration Configuration

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("error reading config file, %s", err)
	}
	err := viper.Unmarshal(&configuration)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}
	return &configuration
}
