package config

import (
	"os"
	"path/filepath"
)

const configName = ".config/proglog"

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(homeDir, configName, filename)
}