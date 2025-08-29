package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	vault "github.com/hashicorp/vault/api"
)

// AppConfig holds runtime configuration loaded from Vault KV exclusively.
type AppConfig struct {
	PostgresHost string
	PostgresPort string
	PostgresDB   string
	PostgresUser string
	PostgresPass string

	KafkaBrokers string

	JWTPrivateKeyPEM string // PEM contents (not a path)
	JWTPublicKeyPEM  string // PEM contents (not a path)

	// gRPC service addresses
	UserServiceAddr   string
	LoggerServiceAddr string

	// Cassandra
	CassandraHosts    string // comma-separated host:port
	CassandraKeyspace string

	// Redis
	RedisHost string
	RedisPort string
	RedisPass string
}

// DSN returns a Postgres connection string computed from the config.
func (c *AppConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPass, c.PostgresHost, c.PostgresPort, c.PostgresDB,
	)
}

// KafkaBrokerList returns Kafka brokers as a slice.
func (c *AppConfig) KafkaBrokerList() []string {
	return splitCommaList(c.KafkaBrokers)
}

// CassandraHostList returns Cassandra hosts as a slice.
func (c *AppConfig) CassandraHostList() []string {
	return splitCommaList(c.CassandraHosts)
}

func splitCommaList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// Vault loading
// -----------------------------------------------------------------------------

// KVPaths defines which Vault paths/keys to read.
// Adjust paths/keys to match your Vault layout. These assume KV v2 mounted at "kv/".
type KVPaths struct {
	// Database
	PGPath string
	PGKeys struct {
		Host string
		Port string
		DB   string
		User string
		Pass string
	}

	// Kafka
	KafkaPath string
	KafkaKeys struct {
		Brokers string
	}

	// JWT
	JWTPath string
	JWTKeys struct {
		PrivatePEM string
		PublicPEM  string
	}

	// Services
	SvcPath string
	SvcKeys struct {
		UserAddr   string
		LoggerAddr string
	}

	// Cassandra
	CassPath string
	CassKeys struct {
		Hosts    string
		Keyspace string
	}

	// Redis
	RedisPath string
	RedisKeys struct {
		Host string
		Port string
		Pass string
	}
}

// DefaultKVPaths returns a reasonable set of defaults that you can tweak.
func DefaultKVPaths() KVPaths {
	var p KVPaths

	p.PGPath = "kv/data/app/postgres"
	p.PGKeys = struct {
		Host string
		Port string
		DB   string
		User string
		Pass string
	}{
		Host: "host", Port: "port", DB: "db", User: "user", Pass: "password",
	}

	p.KafkaPath = "kv/data/app/kafka"
	p.KafkaKeys = struct{ Brokers string }{Brokers: "brokers"}

	p.JWTPath = "kv/data/app/jwt"
	p.JWTKeys = struct {
		PrivatePEM string
		PublicPEM  string
	}{
		PrivatePEM: "private_pem",
		PublicPEM:  "public_pem",
	}

	p.SvcPath = "kv/data/app/services"
	p.SvcKeys = struct {
		UserAddr   string
		LoggerAddr string
	}{
		UserAddr: "user_service_addr", LoggerAddr: "logger_service_addr",
	}

	p.CassPath = "kv/data/app/cassandra"
	p.CassKeys = struct {
		Hosts    string
		Keyspace string
	}{
		Hosts: "hosts", Keyspace: "keyspace",
	}

	p.RedisPath = "kv/data/app/redis"
	p.RedisKeys = struct {
		Host string
		Port string
		Pass string
	}{
		Host: "host", Port: "port", Pass: "password",
	}

	return p
}

// LoadConfigFromVault reads all required values from Vault KV (v2) using the provided client.
func LoadConfigFromVault(ctx context.Context, cli *vault.Client, paths KVPaths) (*AppConfig, error) {
	readKV := func(path string) (map[string]interface{}, error) {
		sec, err := cli.KVv2(extractMount(path)).Get(ctx, trimKVDataPrefix(path))
		if err != nil {
			return nil, fmt.Errorf("vault read %q: %w", path, err)
		}
		return sec.Data, nil
	}

	pg, err := readKV(paths.PGPath)
	if err != nil {
		return nil, err
	}
	kfk, err := readKV(paths.KafkaPath)
	if err != nil {
		return nil, err
	}
	jwt, err := readKV(paths.JWTPath)
	if err != nil {
		return nil, err
	}
	svc, err := readKV(paths.SvcPath)
	if err != nil {
		return nil, err
	}
	cass, err := readKV(paths.CassPath)
	if err != nil {
		return nil, err
	}
	redis, err := readKV(paths.RedisPath)
	if err != nil {
		return nil, err
	}

	cfg := &AppConfig{
		PostgresHost:      mustString(pg, paths.PGKeys.Host),
		PostgresPort:      mustString(pg, paths.PGKeys.Port),
		PostgresDB:        mustString(pg, paths.PGKeys.DB),
		PostgresUser:      mustString(pg, paths.PGKeys.User),
		PostgresPass:      mustString(pg, paths.PGKeys.Pass),
		KafkaBrokers:      mustString(kfk, paths.KafkaKeys.Brokers),
		JWTPrivateKeyPEM:  mustString(jwt, paths.JWTKeys.PrivatePEM),
		JWTPublicKeyPEM:   mustString(jwt, paths.JWTKeys.PublicPEM),
		UserServiceAddr:   mustString(svc, paths.SvcKeys.UserAddr),
		LoggerServiceAddr: mustString(svc, paths.SvcKeys.LoggerAddr),
		CassandraHosts:    mustString(cass, paths.CassKeys.Hosts),
		CassandraKeyspace: mustString(cass, paths.CassKeys.Keyspace),
		RedisHost:         mustString(redis, paths.RedisKeys.Host),
		RedisPort:         mustString(redis, paths.RedisKeys.Port),
		RedisPass:         mustString(redis, paths.RedisKeys.Pass),
	}

	// sanity check JWT keys
	if !looksLikePEM(cfg.JWTPrivateKeyPEM) || !looksLikePEM(cfg.JWTPublicKeyPEM) {
		return nil, fmt.Errorf("jwt keys from vault do not look like PEM")
	}

	return cfg, nil
}

// --- helpers ---

func mustString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		panic(fmt.Errorf("missing required key %q in vault data", key))
	}
	s, ok := v.(string)
	if !ok {
		panic(fmt.Errorf("key %q is not a string in vault data", key))
	}
	return s
}

func looksLikePEM(s string) bool {
	s = strings.TrimSpace(s)
	return strings.HasPrefix(s, "-----BEGIN ") && strings.Contains(s, "-----END ")
}

func extractMount(v2Path string) string {
	parts := strings.Split(v2Path, "/")
	if len(parts) < 1 {
		return v2Path
	}
	return parts[0]
}

func trimKVDataPrefix(v2Path string) string {
	const data = "/data/"
	if i := strings.Index(v2Path, data); i >= 0 {
		return v2Path[i+len(data):]
	}
	return v2Path
}

// RedactableString returns a short fingerprint useful in logs without leaking secrets.
func RedactableString(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:8]) // 8 bytes ~ 16 hex chars
}
