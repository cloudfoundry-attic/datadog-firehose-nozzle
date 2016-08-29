package nozzleconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

type NozzleConfig struct {
	UAAURL                 string
	Client                 string
	ClientSecret           string
	TrafficControllerURL   string
	FirehoseSubscriptionID string
	DataDogURL             string
	DataDogAPIKey          string
	FlushDurationSeconds   uint32
	InsecureSSLSkipVerify  bool
	MetricPrefix           string
	Deployment             string
	DisableAccessControl   bool
	IdleTimeoutSeconds     uint32
}

func Parse(configPath string) (*NozzleConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	var config NozzleConfig
	if err != nil {
		return nil, fmt.Errorf("Can not read config file [%s]: %s", configPath, err)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("Can not parse config file %s: %s", configPath, err)
	}

	overrideWithEnvVar("NOZZLE_UAAURL", &config.UAAURL)
	overrideWithEnvVar("NOZZLE_CLIENT", &config.Client)
	overrideWithEnvVar("NOZZLE_CLIENT_SECRET", &config.ClientSecret)
	overrideWithEnvVar("NOZZLE_TRAFFICCONTROLLERURL", &config.TrafficControllerURL)
	overrideWithEnvVar("NOZZLE_FIREHOSESUBSCRIPTIONID", &config.FirehoseSubscriptionID)
	overrideWithEnvVar("NOZZLE_DATADOGURL", &config.DataDogURL)
	overrideWithEnvVar("NOZZLE_DATADOGAPIKEY", &config.DataDogAPIKey)
	overrideWithEnvVar("NOZZLE_METRICPREFIX", &config.MetricPrefix)
	overrideWithEnvVar("NOZZLE_DEPLOYMENT", &config.Deployment)

	overrideWithEnvUint32("NOZZLE_FLUSHDURATIONSECONDS", &config.FlushDurationSeconds)

	overrideWithEnvBool("NOZZLE_INSECURESSLSKIPVERIFY", &config.InsecureSSLSkipVerify)
	overrideWithEnvBool("NOZZLE_DISABLEACCESSCONTROL", &config.DisableAccessControl)
	overrideWithEnvUint32("NOZZLE_IDLETIMEOUTSECONDS", &config.IdleTimeoutSeconds)
	return &config, nil
}

func overrideWithEnvVar(name string, value *string) {
	envValue := os.Getenv(name)
	if envValue != "" {
		*value = envValue
	}
}

func overrideWithEnvUint32(name string, value *uint32) {
	envValue := os.Getenv(name)
	if envValue != "" {
		tmpValue, err := strconv.Atoi(envValue)
		if err != nil {
			panic(err)
		}
		*value = uint32(tmpValue)
	}
}

func overrideWithEnvBool(name string, value *bool) {
	envValue := os.Getenv(name)
	if envValue != "" {
		var err error
		*value, err = strconv.ParseBool(envValue)
		if err != nil {
			panic(err)
		}
	}
}
