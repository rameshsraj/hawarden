// Package config loads, validates and marshals hawarden's own YAML config.
// Passwords and key material are never stored here: only references
// (*_ref fields pointing at credential files) are allowed.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/rameshsraj/hawarden/internal/exitcode"
)

// DefaultPath is the only config location; -c overrides it.
const DefaultPath = "/etc/hawarden.yaml"

type Role string

const (
	RolePatroni    Role = "patroni"
	RolePG         Role = "pg"
	RoleKeepalived Role = "keepalived"
	RoleEtcd       Role = "etcd"
	RoleHAProxy    Role = "haproxy"
)

// ByteSize is a byte count that marshals as "100GB"-style values.
type ByteSize int64

func (b ByteSize) Int64() int64 { return int64(b) }

func (b ByteSize) MarshalYAML() (any, error) {
	return strconv.FormatInt(int64(b), 10), nil
}

func (b *ByteSize) UnmarshalYAML(v *yaml.Node) error {
	var s string
	if err := v.Decode(&s); err != nil {
		return err
	}
	n, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(n)
	return nil
}

// ParseByteSize accepts "100GB", "512MiB" or a raw byte count.
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	// longest suffixes first so "gb" is tried before "b"
	units := []struct {
		suffix string
		mult   int64
	}{
		{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
		{"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30}, {"tb", 1 << 40},
		{"b", 1},
	}
	lower := strings.ToLower(s)
	for _, u := range units {
		if strings.HasSuffix(lower, u.suffix) {
			n, err := strconv.ParseFloat(strings.TrimSpace(lower[:len(lower)-len(u.suffix)]), 64)
			if err != nil {
				continue // may be a longer-unit name cut short; try the next suffix
			}
			return int64(n * float64(u.mult)), nil
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

type PatroniCfg struct {
	APIPort     int           `yaml:"api_port" json:"api_port"`
	Scheme      string        `yaml:"scheme" json:"scheme"` // http|https
	User        string        `yaml:"user,omitempty" json:"user,omitempty"`
	PasswordRef string        `yaml:"password_ref,omitempty" json:"password_ref,omitempty"`
	Timeout     time.Duration `yaml:"timeout" json:"timeout"`
}

type EtcdCfg struct {
	Endpoints   []string      `yaml:"endpoints" json:"endpoints"`
	Bin         string        `yaml:"bin,omitempty" json:"bin,omitempty"`
	DialTimeout time.Duration `yaml:"dial_timeout" json:"dial_timeout"`
	CACertRef   string        `yaml:"cacert_ref,omitempty" json:"cacert_ref,omitempty"`
	CertRef     string        `yaml:"cert_ref,omitempty" json:"cert_ref,omitempty"`
	KeyRef      string        `yaml:"key_ref,omitempty" json:"key_ref,omitempty"`
	Metrics     bool          `yaml:"metrics" json:"metrics"`
}

type VIP struct {
	Address   string `yaml:"address" json:"address"`
	Interface string `yaml:"interface" json:"interface"`
	Policy    string `yaml:"policy" json:"policy"` // leader_only|proxy_tier|user_defined
}

// ProxyListener is one HAProxy frontend whose backend selection follows a
// Patroni REST health check (HAProxy `option httpchk GET <check>`).
type ProxyListener struct {
	Name  string `yaml:"name" json:"name"`
	Port  int    `yaml:"port" json:"port"`
	Check string `yaml:"check" json:"check"` // e.g. /primary, /replica
}

type SSH struct {
	Key           string        `yaml:"key" json:"key"`
	KnownHosts    string        `yaml:"known_hosts" json:"known_hosts"`
	Timeout       time.Duration `yaml:"timeout" json:"timeout"`
	StrictHostKey bool          `yaml:"strict_host_key" json:"strict_host_key"`
}

type Lag struct {
	WarnSeconds int `yaml:"warn_seconds" json:"warn_seconds"`
	CritSeconds int `yaml:"crit_seconds" json:"crit_seconds"`
}

type Audit struct {
	File          string `yaml:"file" json:"file"`
	Syslog        bool   `yaml:"syslog" json:"syslog"`
	ReadCommands  bool   `yaml:"read_commands" json:"read_commands"`
	RetentionDays int    `yaml:"retention_days" json:"retention_days"`
}

type History struct {
	RetentionDays int           `yaml:"retention_days" json:"retention_days"`
	SnapshotEvery time.Duration `yaml:"snapshot_every" json:"snapshot_every"`
}

// PGCfg is the optional least-privilege replication context adapter. Empty
// Adapter means "disabled" — no PostgreSQL collection at all.
type PGCfg struct {
	Adapter string `yaml:"adapter,omitempty" json:"adapter,omitempty"` // ssh-psql (only supported value)
	User    string `yaml:"user,omitempty" json:"user,omitempty"`
	Port    int    `yaml:"port,omitempty" json:"port,omitempty"`
	DataDir string `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	Service string `yaml:"service,omitempty" json:"service,omitempty"` // systemctl unit name
}

// HostPatroni is per-host Patroni context.
type HostPatroni struct {
	Conf    string `yaml:"conf,omitempty" json:"conf,omitempty"`
	LogDir  string `yaml:"log_dir,omitempty" json:"log_dir,omitempty"`
	Service string `yaml:"service,omitempty" json:"service,omitempty"`
	API     string `yaml:"api,omitempty" json:"api,omitempty"` // explicit base URL, overrides scheme/address/port
}

// HostKeepalived is per-host keepalived context.
type HostKeepalived struct {
	Conf        string `yaml:"conf,omitempty" json:"conf,omitempty"`
	Service     string `yaml:"service,omitempty" json:"service,omitempty"`
	JournalUnit string `yaml:"journal_unit,omitempty" json:"journal_unit,omitempty"`
	StateFile   string `yaml:"state_file,omitempty" json:"state_file,omitempty"` // notify-script state file
}

// HostEtcd is per-host etcd context.
type HostEtcd struct {
	Name    string `yaml:"name,omitempty" json:"name,omitempty"`
	Service string `yaml:"service,omitempty" json:"service,omitempty"`
	DataDir string `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
}

type Host struct {
	Name        string          `yaml:"name" json:"name"`
	Address     string          `yaml:"address" json:"address"`
	Roles       []Role          `yaml:"roles" json:"roles"`
	SSHUser     string          `yaml:"ssh_user" json:"ssh_user"`
	SSHDisabled bool            `yaml:"ssh_disabled,omitempty" json:"ssh_disabled,omitempty"` // no SSH telemetry on this machine
	Patroni     *HostPatroni    `yaml:"patroni,omitempty" json:"patroni,omitempty"`
	PG          *PGCfg          `yaml:"pg,omitempty" json:"pg,omitempty"`
	Keepalived  *HostKeepalived `yaml:"keepalived,omitempty" json:"keepalived,omitempty"`
	Etcd        *HostEtcd       `yaml:"etcd,omitempty" json:"etcd,omitempty"`
}

func (h Host) Has(r Role) bool {
	for _, x := range h.Roles {
		if x == r {
			return true
		}
	}
	return false
}

type Config struct {
	Cluster            string            `yaml:"cluster" json:"cluster"`
	StateDir           string            `yaml:"state_dir" json:"state_dir"`
	Operators          map[string]string `yaml:"operators,omitempty" json:"operators,omitempty"`
	AllowedOperators   []string          `yaml:"allowed_operators" json:"allowed_operators"`
	Patroni            PatroniCfg        `yaml:"patroni" json:"patroni"`
	Etcd               EtcdCfg           `yaml:"etcd" json:"etcd"`
	VIPs               []VIP             `yaml:"vips" json:"vips"`
	Proxy              []ProxyListener   `yaml:"proxy,omitempty" json:"proxy,omitempty"`
	SSH                SSH               `yaml:"ssh" json:"ssh"`
	Interval           time.Duration     `yaml:"interval" json:"interval"`
	Timeout            time.Duration     `yaml:"timeout" json:"timeout"`
	Lag                Lag               `yaml:"lag" json:"lag"`
	WALVolumeBytes     ByteSize          `yaml:"wal_volume_bytes" json:"wal_volume_bytes"`
	ReplicationContext bool              `yaml:"replication_context" json:"replication_context"`
	Audit              Audit             `yaml:"audit" json:"audit"`
	History            History           `yaml:"history" json:"history"`
	Hosts              []Host            `yaml:"hosts" json:"hosts"`
}

// Defaults is the full default configuration; user YAML decodes over it.
func Defaults() Config {
	return Config{
		StateDir: "/var/lib/hawarden",
		Patroni: PatroniCfg{
			APIPort: 8008, Scheme: "http", Timeout: 3 * time.Second,
		},
		Etcd: EtcdCfg{
			DialTimeout: 3 * time.Second,
			Metrics:     true,
		},
		SSH: SSH{
			Key:        "/root/.ssh/id_ed25519",
			KnownHosts: "/root/.ssh/known_hosts",
			Timeout:    5 * time.Second,
		},
		Interval:       2 * time.Second,
		Timeout:        3 * time.Second,
		Lag:            Lag{WarnSeconds: 1800, CritSeconds: 3600},
		WALVolumeBytes: ByteSize(100 << 30),
		Audit: Audit{
			File:          "/var/lib/hawarden/audit.jsonl",
			Syslog:        true,
			RetentionDays: 31,
		},
		History:          History{RetentionDays: 14, SnapshotEvery: 30 * time.Second},
		AllowedOperators: []string{},
	}
}

// ApplyDefaults fills per-host fields that depend on the host's roles.
func (c *Config) ApplyDefaults() {
	for i := range c.Hosts {
		h := &c.Hosts[i]
		if h.SSHUser == "" {
			h.SSHUser = "root"
		}
		if h.Has(RolePatroni) && h.Patroni == nil {
			h.Patroni = &HostPatroni{Service: "patroni"}
		}
		if h.Has(RolePG) && h.PG == nil {
			h.PG = &PGCfg{Port: 5432}
		}
		if h.Has(RoleKeepalived) && h.Keepalived == nil {
			h.Keepalived = &HostKeepalived{Service: "keepalived", JournalUnit: "keepalived"}
		}
		if h.Has(RoleEtcd) && h.Etcd == nil {
			h.Etcd = &HostEtcd{Service: "etcd"}
		}
	}
}

// Load reads path for the real process uid.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, exitcode.New(exitcode.Usage, "config file not found").
				WithCause(path).
				WithHint("hawarden config init")
		}
		return nil, exitcode.New(exitcode.Usage, "cannot read config file").
			WithCause(err.Error()).WithHint(path)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Parse decodes YAML over Defaults and validates.
func Parse(data []byte) (*Config, error) {
	cfg := Defaults()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && err != io.EOF {
		return nil, exitcode.New(exitcode.Usage, "invalid config file").
			WithCause(err.Error())
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks every rule; all problems are returned in one error, one
// per line, each prefixed with its field path.
func (c *Config) Validate() error {
	var p []string
	bad := func(s string) { p = append(p, s) }
	badf := func(format string, args ...any) { bad(fmt.Sprintf(format, args...)) }

	if c.Cluster == "" {
		bad("cluster: must not be empty")
	}
	if len(c.Hosts) == 0 {
		bad("hosts: must define at least one host")
	}
	seen := map[string]bool{}
	for i, h := range c.Hosts {
		base := fmt.Sprintf("hosts[%d]", i)
		if h.Name == "" {
			bad(base + ".name: must not be empty")
		} else if seen[h.Name] {
			badf(base+".name: %q is duplicated", h.Name)
		}
		seen[h.Name] = true
		if h.Address == "" {
			bad(base + ".address: must not be empty")
		}
		if len(h.Roles) == 0 {
			bad(base + ".roles: must have at least one role")
		}
		for j, r := range h.Roles {
			switch r {
			case RolePatroni, RolePG, RoleKeepalived, RoleEtcd, RoleHAProxy:
			default:
				badf("%s.roles[%d]: %q must be patroni|pg|keepalived|etcd|haproxy", base, j, string(r))
			}
		}
		if h.PG != nil && h.PG.Adapter != "" && h.PG.Adapter != "ssh-psql" {
			badf(base+".pg.adapter: %q unsupported (only ssh-psql)", h.PG.Adapter)
		}
	}
	if c.Lag.WarnSeconds <= 0 || c.Lag.CritSeconds <= 0 {
		bad("lag: warn_seconds and crit_seconds must be > 0")
	} else if c.Lag.WarnSeconds >= c.Lag.CritSeconds {
		bad("lag: warn_seconds must be < crit_seconds")
	}
	if c.Interval <= 0 {
		bad("interval: must be > 0")
	}
	if c.Timeout <= 0 {
		bad("timeout: must be > 0")
	}
	for i, v := range c.VIPs {
		if net.ParseIP(v.Address) == nil {
			badf("vips[%d].address: %q is not an IP address", i, v.Address)
		}
		switch v.Policy {
		case "", "leader_only", "proxy_tier", "user_defined":
		default:
			badf("vips[%d].policy: %q must be leader_only|proxy_tier|user_defined", i, v.Policy)
		}
	}
	for i, l := range c.Proxy {
		if l.Name == "" {
			badf("proxy[%d].name: must not be empty", i)
		}
		if l.Port < 1 || l.Port > 65535 {
			badf("proxy[%d].port: %d out of range", i, l.Port)
		}
		if !strings.HasPrefix(l.Check, "/") {
			badf("proxy[%d].check: %q must be a Patroni REST path like /primary", i, l.Check)
		}
	}
	for _, ep := range c.Etcd.Endpoints {
		if _, _, err := net.SplitHostPort(ep); err != nil {
			badf("etcd.endpoints: %q must be host:port", ep)
		}
	}
	for _, f := range [][2]string{
		{"state_dir", c.StateDir},
		{"ssh.key", c.SSH.Key},
		{"ssh.known_hosts", c.SSH.KnownHosts},
		{"audit.file", c.Audit.File},
		{"etcd.cacert_ref", c.Etcd.CACertRef},
		{"etcd.cert_ref", c.Etcd.CertRef},
		{"etcd.key_ref", c.Etcd.KeyRef},
	} {
		if f[1] != "" && !filepath.IsAbs(f[1]) {
			bad(f[0] + ": must be absolute")
		}
	}
	if (c.Etcd.CertRef == "") != (c.Etcd.KeyRef == "") {
		bad("etcd: cert_ref and key_ref must be set together")
	}
	if len(p) > 0 {
		return exitcode.New(exitcode.Usage, "invalid config").
			WithCause(strings.Join(p, "\n"))
	}
	return nil
}

func (c *Config) HostsWith(r Role) []Host {
	var out []Host
	for _, h := range c.Hosts {
		if h.Has(r) {
			out = append(out, h)
		}
	}
	return out
}

func (c *Config) HostByName(name string) (Host, bool) {
	for _, h := range c.Hosts {
		if h.Name == name {
			return h, true
		}
	}
	return Host{}, false
}

// PatroniAPI resolves the Patroni REST base URL for a host.
func (c *Config) PatroniAPI(h Host) string {
	if h.Patroni != nil && h.Patroni.API != "" {
		return strings.TrimSuffix(h.Patroni.API, "/")
	}
	scheme := c.Patroni.Scheme
	if scheme == "" {
		scheme = "http"
	}
	port := c.Patroni.APIPort
	if port == 0 {
		port = 8008
	}
	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(h.Address, strconv.Itoa(port)))
}

// Marshal renders the config back to YAML.
func (c *Config) Marshal() ([]byte, error) {
	var b bytes.Buffer
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return b.Bytes(), enc.Close()
}

// AtomicWrite writes data to path via temp file + fsync + rename.
func AtomicWrite(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".hawarden-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
