package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/go-randgen/gendata"
	"github.com/pingcap/go-randgen/grammar"
	"github.com/pingcap/go-randgen/grammar/sql_generator"
)

type Config struct {
	SQLGen SQLGenConfig `toml:"sql_gen"`
	Schema SchemaConfig `toml:"schema"`
}

type SQLGenConfig struct {
	Seed         int64  `toml:"seed"`
	Queries      int    `toml:"queries"`
	MaxRecursive int    `toml:"max_recursive"`
	YYTemplate   string `toml:"yy_template"`
	Output       string `toml:"output"`
}

type SchemaConfig struct {
	IntColumns    int    `toml:"int_columns"`
	BoolColumns   int    `toml:"bool_columns"`
	StringColumns int    `toml:"string_columns"`
	TableName     string `toml:"table_name"`
}

func main() {
	// Default to the sibling config file so `go -C tests/sql_assert/sql_gen run .` works.
	defaultConfig := "../config-test.toml"
	configPath := flag.String("config", defaultConfig, "path to config.toml")
	flag.Parse()

	cfg, baseDir, err := loadConfig(*configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}

	if err := validateConfig(&cfg); err != nil {
		fatalf("invalid config: %v", err)
	}

	yyPath := resolvePath(baseDir, cfg.SQLGen.YYTemplate)
	outPath := resolvePath(baseDir, cfg.SQLGen.Output)

	yyTemplate, err := os.ReadFile(yyPath)
	if err != nil {
		fatalf("read yy template: %v", err)
	}

	intCols := buildColumns("c_int_", cfg.Schema.IntColumns)
	boolCols := buildColumns("c_bool_", cfg.Schema.BoolColumns)
	strCols := buildColumns("c_str_", cfg.Schema.StringColumns)
	allCols := append(append([]string{}, intCols...), append(boolCols, strCols...)...)

	replacements := map[string]string{
		"{{all_cols}}":   formatLuaList(allCols),
		"{{int_cols}}":   formatLuaList(intCols),
		"{{bool_cols}}":  formatLuaList(boolCols),
		"{{str_cols}}":   formatLuaList(strCols),
		"{{seed}}":       fmt.Sprintf("%d", cfg.SQLGen.Seed),
		"{{table_name}}": cfg.Schema.TableName,
	}

	yy := applyTemplate(string(yyTemplate), replacements)

	keyfun := gendata.NewKeyfun(nil, nil)
	iter, err := grammar.NewIterWithRander(
		yy,
		"query",
		cfg.SQLGen.MaxRecursive,
		keyfun,
		rand.New(rand.NewSource(cfg.SQLGen.Seed)),
		false,
	)
	if err != nil {
		fatalf("build sql iterator: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		fatalf("create output dir: %v", err)
	}
	file, err := os.Create(outPath)
	if err != nil {
		fatalf("create output file: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	count := 0
	visitor := sql_generator.FixedTimesVisitor(func(_ int, sql string) {
		clean := sanitizeSQL(sql)
		if clean == "" {
			return
		}
		_, _ = writer.WriteString(clean + "\n")
		count++
	}, cfg.SQLGen.Queries)

	if err := iter.Visit(visitor); err != nil {
		fatalf("generate sql: %v", err)
	}

	fmt.Printf("generated %d SQL statements at %s\n", count, outPath)
}

func loadConfig(path string) (Config, string, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, "", err
	}
	baseDir := filepath.Dir(path)
	return cfg, baseDir, nil
}

func validateConfig(cfg *Config) error {
	if cfg.SQLGen.Queries < 0 {
		return fmt.Errorf("queries must be >= 0")
	}
	if cfg.SQLGen.MaxRecursive <= 0 {
		cfg.SQLGen.MaxRecursive = 5
	}
	if cfg.SQLGen.YYTemplate == "" {
		return fmt.Errorf("yy_template is required")
	}
	if cfg.SQLGen.Output == "" {
		return fmt.Errorf("output is required")
	}
	if cfg.Schema.IntColumns <= 0 || cfg.Schema.BoolColumns <= 0 || cfg.Schema.StringColumns <= 0 {
		return fmt.Errorf("column counts must be >= 1")
	}
	if cfg.Schema.TableName == "" {
		cfg.Schema.TableName = "stream"
	}
	return nil
}

func buildColumns(prefix string, count int) []string {
	cols := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		cols = append(cols, fmt.Sprintf("%s%d", prefix, i))
	}
	return cols
}

func formatLuaList(cols []string) string {
	parts := make([]string, 0, len(cols))
	for _, col := range cols {
		parts = append(parts, fmt.Sprintf("\"%s\"", col))
	}
	return strings.Join(parts, ", ")
}

func applyTemplate(input string, replacements map[string]string) string {
	out := input
	for key, value := range replacements {
		out = strings.ReplaceAll(out, key, value)
	}
	return out
}

func sanitizeSQL(sql string) string {
	out := strings.ReplaceAll(sql, "\n", " ")
	out = strings.ReplaceAll(out, "\r", " ")
	out = strings.TrimSpace(out)
	out = strings.TrimSuffix(out, ";")
	return strings.Join(strings.Fields(out), " ")
}

func resolvePath(baseDir, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(baseDir, path)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
