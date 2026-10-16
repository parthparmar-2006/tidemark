package config

import (
	"os"
	"strconv"
	"time"
)

func String(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func Int(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return number
}

func Duration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}

func DatabaseURL() string {
	return String("DATABASE_URL", "postgres://tidemark:tidemark@localhost:5433/tidemark?sslmode=disable")
}

func KafkaBrokers() string {
	return String("KAFKA_BROKERS", "localhost:9092")
}

func Topic() string {
	return String("TOPIC", "account-events")
}
