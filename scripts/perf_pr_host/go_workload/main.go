package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const randomAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type runConfig struct {
	BrokerURL     string
	TopicCritical string
	TopicBest     string
	Columns       int
	Cases         int
	StrLen        int
	QoS           int
	DurationSec   int
	QPS           int
	QPSCritical   int
	QPSBest       int
	ClientPrefix  string
}

type controlConfig struct {
	BaseURL                string
	TimeoutSec             int
	HTTPRetries            int
	HTTPRetryIntervalMS    int
	Strict                 bool
	StreamNameCritical     string
	StreamNameBest         string
	PipelineIDCritical     string
	PipelineIDBest         string
	BrokerURL              string
	TopicCritical          string
	TopicBest              string
	Columns                int
	QoS                    int
	SQLMode                string
	CriticalInstanceID     string
	BestInstanceID         string
	StopMode               string
	StopTimeoutMS          int
	StreamDeleteRetries    int
	StreamDeleteIntervalMS int
}

type publisherResult struct {
	Label    string
	Topic    string
	Rate     int
	Duration time.Duration
	Sent     int64
	Err      error
}

type apiClient struct {
	baseURL string
	client  *http.Client
}

type opResult struct {
	warnings []string
	errors   []string
}

type streamSpec struct {
	Name  string
	Topic string
}

type pipelineSpec struct {
	ID             string
	StreamName     string
	FlowInstanceID string
}

func main() {
	command, args, err := detectCommand(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	switch command {
	case "run":
		cfg, err := parseRunFlags(args)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := validateRunConfig(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := runWorkload(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	case "provision":
		cfg, err := parseControlFlags("provision", args)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := validateControlConfig("provision", cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := runProvision(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	case "start":
		cfg, err := parseControlFlags("start", args)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := validateControlConfig("start", cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := runStart(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	case "pause":
		cfg, err := parseControlFlags("pause", args)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := validateControlConfig("pause", cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := runPause(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	case "delete":
		cfg, err := parseControlFlags("delete", args)
		if err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := validateControlConfig("delete", cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		if err := runDelete(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	default:
		fmt.Fprintf(os.Stderr, "error: unknown command: %s\n", command)
		os.Exit(2)
	}
}

func detectCommand(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "run", args, nil
	}
	first := args[0]
	if strings.HasPrefix(first, "-") {
		return "run", args, nil
	}
	switch first {
	case "run", "provision", "start", "pause", "delete":
		return first, args[1:], nil
	default:
		return "", nil, fmt.Errorf("unknown command: %s", first)
	}
}

func parseRunFlags(args []string) (*runConfig, error) {
	cfg := &runConfig{}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	fs.StringVar(&cfg.BrokerURL, "broker-url", "tcp://127.0.0.1:1883", "MQTT broker URL")
	fs.StringVar(&cfg.TopicCritical, "topic-critical", "/perf/pr/critical", "critical topic")
	fs.StringVar(&cfg.TopicBest, "topic-best", "/perf/pr/best", "best-effort topic")
	fs.IntVar(&cfg.Columns, "columns", 15000, "number of JSON columns (a1..aN)")
	fs.IntVar(&cfg.Cases, "cases", 20, "number of payload cases")
	fs.IntVar(&cfg.StrLen, "str-len", 10, "string length for each column value")
	fs.IntVar(&cfg.QoS, "qos", 1, "MQTT QoS, only 1 is supported")
	fs.IntVar(&cfg.DurationSec, "duration-secs", 60, "workload duration seconds")
	fs.IntVar(&cfg.QPS, "qps", 20, "default publish rate for critical and best")
	fs.IntVar(&cfg.QPSCritical, "qps-critical", -1, "critical publish rate override")
	fs.IntVar(&cfg.QPSBest, "qps-best", -1, "best publish rate override")
	fs.StringVar(&cfg.ClientPrefix, "client-prefix", "perf-pr-go", "MQTT client id prefix")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected positional args: %v", fs.Args())
	}
	return cfg, nil
}

func parseControlFlags(command string, args []string) (*controlConfig, error) {
	cfg := &controlConfig{}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(os.Stdout)

	fs.StringVar(&cfg.BaseURL, "base-url", "http://127.0.0.1:8080", "manager base URL")
	fs.IntVar(&cfg.TimeoutSec, "timeout-secs", 60, "HTTP timeout seconds")
	fs.IntVar(&cfg.HTTPRetries, "http-retries", 3, "HTTP retry attempts")
	fs.IntVar(&cfg.HTTPRetryIntervalMS, "http-retry-interval-ms", 300, "HTTP retry interval milliseconds")
	fs.BoolVar(&cfg.Strict, "strict", true, "strict final consistency check")

	fs.StringVar(&cfg.StreamNameCritical, "stream-name-critical", "perf_pr_stream_critical", "critical stream name")
	fs.StringVar(&cfg.StreamNameBest, "stream-name-best", "perf_pr_stream_best", "best stream name")
	fs.StringVar(&cfg.PipelineIDCritical, "pipeline-id-critical", "perf_pr_pipeline_critical", "critical pipeline id")
	fs.StringVar(&cfg.PipelineIDBest, "pipeline-id-best", "perf_pr_pipeline_best", "best pipeline id")

	fs.StringVar(&cfg.BrokerURL, "broker-url", "tcp://127.0.0.1:1883", "MQTT broker URL")
	fs.StringVar(&cfg.TopicCritical, "topic-critical", "/perf/pr/critical", "critical topic")
	fs.StringVar(&cfg.TopicBest, "topic-best", "/perf/pr/best", "best topic")
	fs.IntVar(&cfg.Columns, "columns", 15000, "stream column count")
	fs.IntVar(&cfg.QoS, "qos", 1, "stream qos")
	fs.StringVar(&cfg.SQLMode, "sql-mode", "explicit", "explicit|star")
	fs.StringVar(&cfg.CriticalInstanceID, "critical-instance-id", "fi_critical", "critical flow instance id")
	fs.StringVar(&cfg.BestInstanceID, "best-instance-id", "fi_best", "best flow instance id")

	fs.StringVar(&cfg.StopMode, "stop-mode", "graceful", "pipeline stop mode")
	fs.IntVar(&cfg.StopTimeoutMS, "stop-timeout-ms", 5000, "pipeline stop timeout ms")
	fs.IntVar(&cfg.StreamDeleteRetries, "stream-delete-retries", 5, "stream delete retries")
	fs.IntVar(&cfg.StreamDeleteIntervalMS, "stream-delete-interval-ms", 1000, "stream delete retry interval milliseconds")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected positional args: %v", fs.Args())
	}
	return cfg, nil
}

func validateRunConfig(cfg *runConfig) error {
	if cfg.Columns <= 0 {
		return errors.New("--columns must be > 0")
	}
	if cfg.Cases <= 0 {
		return errors.New("--cases must be > 0")
	}
	if cfg.StrLen <= 0 {
		return errors.New("--str-len must be > 0")
	}
	if cfg.QoS != 1 {
		return errors.New("--qos must be 1")
	}
	if cfg.DurationSec < 0 {
		return errors.New("--duration-secs must be >= 0")
	}
	if cfg.QPS < 0 {
		return errors.New("--qps must be >= 0")
	}
	if cfg.QPSCritical < -1 {
		return errors.New("--qps-critical must be >= -1")
	}
	if cfg.QPSBest < -1 {
		return errors.New("--qps-best must be >= -1")
	}
	if strings.TrimSpace(cfg.TopicCritical) == "" || strings.TrimSpace(cfg.TopicBest) == "" {
		return errors.New("topic must not be empty")
	}
	if strings.TrimSpace(cfg.ClientPrefix) == "" {
		return errors.New("--client-prefix must not be empty")
	}
	u, err := url.Parse(cfg.BrokerURL)
	if err != nil {
		return fmt.Errorf("invalid --broker-url: %w", err)
	}
	if u.Scheme != "tcp" || u.Host == "" {
		return errors.New("--broker-url must be tcp://host:port")
	}
	return nil
}

func validateControlConfig(command string, cfg *controlConfig) error {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return errors.New("--base-url must not be empty")
	}
	if cfg.TimeoutSec <= 0 {
		return errors.New("--timeout-secs must be > 0")
	}
	if cfg.HTTPRetries <= 0 {
		return errors.New("--http-retries must be > 0")
	}
	if cfg.HTTPRetryIntervalMS < 0 {
		return errors.New("--http-retry-interval-ms must be >= 0")
	}
	if cfg.StreamDeleteRetries <= 0 {
		return errors.New("--stream-delete-retries must be > 0")
	}
	if cfg.StreamDeleteIntervalMS < 0 {
		return errors.New("--stream-delete-interval-ms must be >= 0")
	}
	if cfg.StopTimeoutMS < 0 {
		return errors.New("--stop-timeout-ms must be >= 0")
	}
	if strings.TrimSpace(cfg.StopMode) == "" {
		return errors.New("--stop-mode must not be empty")
	}
	if strings.TrimSpace(cfg.StreamNameCritical) == "" || strings.TrimSpace(cfg.StreamNameBest) == "" {
		return errors.New("stream name must not be empty")
	}
	if strings.TrimSpace(cfg.PipelineIDCritical) == "" || strings.TrimSpace(cfg.PipelineIDBest) == "" {
		return errors.New("pipeline id must not be empty")
	}
	if command == "provision" {
		if cfg.Columns <= 0 {
			return errors.New("--columns must be > 0")
		}
		if cfg.QoS < 0 || cfg.QoS > 2 {
			return errors.New("--qos must be in range [0,2]")
		}
		if cfg.SQLMode != "explicit" && cfg.SQLMode != "star" {
			return errors.New("--sql-mode must be explicit|star")
		}
		if strings.TrimSpace(cfg.CriticalInstanceID) == "" || strings.TrimSpace(cfg.BestInstanceID) == "" {
			return errors.New("flow instance id must not be empty")
		}
	}
	return nil
}

func runWorkload(cfg *runConfig) error {
	payloads, err := generatePayloadCases(cfg.Columns, cfg.Cases, cfg.StrLen)
	if err != nil {
		return err
	}
	rateCritical := cfg.QPS
	if cfg.QPSCritical >= 0 {
		rateCritical = cfg.QPSCritical
	}
	rateBest := cfg.QPS
	if cfg.QPSBest >= 0 {
		rateBest = cfg.QPSBest
	}

	if err := runSingle(cfg, payloads, cfg.DurationSec, rateCritical, rateBest); err != nil {
		return err
	}

	fmt.Printf("[workload] done duration=%ds rate_critical=%d rate_best=%d\n", cfg.DurationSec, rateCritical, rateBest)
	return nil
}

func runProvision(cfg *controlConfig) error {
	client := newAPIClient(cfg.BaseURL, cfg.TimeoutSec)
	res := &opResult{}

	streams := []streamSpec{
		{Name: cfg.StreamNameCritical, Topic: cfg.TopicCritical},
		{Name: cfg.StreamNameBest, Topic: cfg.TopicBest},
	}
	pipelines := []pipelineSpec{
		{ID: cfg.PipelineIDCritical, StreamName: cfg.StreamNameCritical, FlowInstanceID: cfg.CriticalInstanceID},
		{ID: cfg.PipelineIDBest, StreamName: cfg.StreamNameBest, FlowInstanceID: cfg.BestInstanceID},
	}

	for _, pipeline := range pipelines {
		pausePipeline(client, cfg, pipeline.ID, res, true)
		deletePipeline(client, cfg, pipeline.ID, res, true)
	}
	for _, stream := range streams {
		deleteStream(client, cfg, stream.Name, res, true)
	}
	for _, stream := range streams {
		createStream(client, cfg, stream, res)
	}
	for _, pipeline := range pipelines {
		createPipeline(client, cfg, pipeline, res)
	}

	verifyPresence(client, cfg, streams, pipelines, res)
	return finalizeResult("provision", cfg.Strict, res)
}

func runStart(cfg *controlConfig) error {
	client := newAPIClient(cfg.BaseURL, cfg.TimeoutSec)
	res := &opResult{}

	pipelines := []string{cfg.PipelineIDCritical, cfg.PipelineIDBest}
	for _, pipelineID := range pipelines {
		startPipeline(client, cfg, pipelineID, res)
	}

	if cfg.Strict {
		set, err := listPipelines(client, cfg)
		if err != nil {
			res.errors = append(res.errors, fmt.Sprintf("start verify failed: %v", err))
		} else {
			for _, pipelineID := range pipelines {
				if _, ok := set[pipelineID]; !ok {
					res.errors = append(res.errors, fmt.Sprintf("pipeline missing after start: %s", pipelineID))
				}
			}
		}
	}

	return finalizeResult("start", cfg.Strict, res)
}

func runPause(cfg *controlConfig) error {
	client := newAPIClient(cfg.BaseURL, cfg.TimeoutSec)
	res := &opResult{}

	pipelines := []string{cfg.PipelineIDCritical, cfg.PipelineIDBest}
	for _, pipelineID := range pipelines {
		pausePipeline(client, cfg, pipelineID, res, true)
	}

	return finalizeResult("pause", cfg.Strict, res)
}

func runDelete(cfg *controlConfig) error {
	client := newAPIClient(cfg.BaseURL, cfg.TimeoutSec)
	res := &opResult{}

	pipelines := []string{cfg.PipelineIDCritical, cfg.PipelineIDBest}
	streams := []string{cfg.StreamNameCritical, cfg.StreamNameBest}

	for _, pipelineID := range pipelines {
		pausePipeline(client, cfg, pipelineID, res, true)
	}
	for _, pipelineID := range pipelines {
		deletePipeline(client, cfg, pipelineID, res, true)
	}
	for _, streamName := range streams {
		deleteStream(client, cfg, streamName, res, true)
	}

	verifyAbsence(client, cfg, streams, pipelines, res)
	return finalizeResult("delete", cfg.Strict, res)
}

func newAPIClient(baseURL string, timeoutSec int) *apiClient {
	return &apiClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: time.Duration(timeoutSec) * time.Second,
		},
	}
}

func (c *apiClient) doJSON(method, path string, query url.Values, body any) (int, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		bodyReader = bytes.NewReader(raw)
	}

	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequest(method, u, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

func callWithRetry(
	cfg *controlConfig,
	fn func() (int, []byte, error),
) (int, []byte, error) {
	interval := time.Duration(cfg.HTTPRetryIntervalMS) * time.Millisecond
	var lastStatus int
	var lastBody []byte
	var lastErr error

	for attempt := 1; attempt <= cfg.HTTPRetries; attempt++ {
		status, body, err := fn()
		lastStatus = status
		lastBody = body
		lastErr = err

		retryable := err != nil || status >= 500
		if !retryable || attempt == cfg.HTTPRetries {
			return status, body, err
		}
		if interval > 0 {
			time.Sleep(interval)
		}
	}
	return lastStatus, lastBody, lastErr
}

func startPipeline(client *apiClient, cfg *controlConfig, pipelineID string, res *opResult) {
	path := "/pipelines/" + url.PathEscape(pipelineID) + "/start"
	status, body, err := callWithRetry(cfg, func() (int, []byte, error) {
		return client.doJSON(http.MethodPost, path, nil, nil)
	})
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("start %s request failed: %v", pipelineID, err))
		return
	}
	switch status {
	case http.StatusOK:
		fmt.Printf("[resource] start pipeline=%s status=%d\n", pipelineID, status)
	case http.StatusNotFound:
		msg := fmt.Sprintf("start pipeline=%s not found", pipelineID)
		if cfg.Strict {
			res.errors = append(res.errors, msg)
		} else {
			res.warnings = append(res.warnings, msg)
		}
	default:
		res.errors = append(res.errors, fmt.Sprintf("start %s failed: HTTP %d: %s", pipelineID, status, trimBody(body)))
	}
}

func pausePipeline(client *apiClient, cfg *controlConfig, pipelineID string, res *opResult, notFoundOK bool) {
	path := "/pipelines/" + url.PathEscape(pipelineID) + "/stop"
	query := url.Values{
		"mode":       []string{cfg.StopMode},
		"timeout_ms": []string{fmt.Sprintf("%d", cfg.StopTimeoutMS)},
	}
	status, body, err := callWithRetry(cfg, func() (int, []byte, error) {
		return client.doJSON(http.MethodPost, path, query, nil)
	})
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("pause %s request failed: %v", pipelineID, err))
		return
	}
	switch status {
	case http.StatusOK:
		fmt.Printf("[resource] pause pipeline=%s status=%d\n", pipelineID, status)
	case http.StatusNotFound:
		msg := fmt.Sprintf("pause pipeline=%s not found (skip)", pipelineID)
		if notFoundOK {
			res.warnings = append(res.warnings, msg)
		} else if cfg.Strict {
			res.errors = append(res.errors, msg)
		} else {
			res.warnings = append(res.warnings, msg)
		}
	default:
		res.errors = append(res.errors, fmt.Sprintf("pause %s failed: HTTP %d: %s", pipelineID, status, trimBody(body)))
	}
}

func deletePipeline(client *apiClient, cfg *controlConfig, pipelineID string, res *opResult, notFoundOK bool) {
	path := "/pipelines/" + url.PathEscape(pipelineID)
	status, body, err := callWithRetry(cfg, func() (int, []byte, error) {
		return client.doJSON(http.MethodDelete, path, nil, nil)
	})
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("delete pipeline %s request failed: %v", pipelineID, err))
		return
	}
	switch status {
	case http.StatusOK:
		fmt.Printf("[resource] delete pipeline=%s status=%d\n", pipelineID, status)
	case http.StatusNotFound:
		msg := fmt.Sprintf("delete pipeline=%s not found (skip)", pipelineID)
		if notFoundOK {
			res.warnings = append(res.warnings, msg)
		} else if cfg.Strict {
			res.errors = append(res.errors, msg)
		} else {
			res.warnings = append(res.warnings, msg)
		}
	default:
		res.errors = append(res.errors, fmt.Sprintf("delete pipeline %s failed: HTTP %d: %s", pipelineID, status, trimBody(body)))
	}
}

func deleteStream(client *apiClient, cfg *controlConfig, streamName string, res *opResult, notFoundOK bool) {
	path := "/streams/" + url.PathEscape(streamName)
	interval := time.Duration(cfg.StreamDeleteIntervalMS) * time.Millisecond

	for attempt := 1; attempt <= cfg.StreamDeleteRetries; attempt++ {
		status, body, err := callWithRetry(cfg, func() (int, []byte, error) {
			return client.doJSON(http.MethodDelete, path, nil, nil)
		})
		if err != nil {
			res.errors = append(res.errors, fmt.Sprintf("delete stream %s request failed: %v", streamName, err))
			return
		}
		switch status {
		case http.StatusOK:
			fmt.Printf("[resource] delete stream=%s status=%d\n", streamName, status)
			return
		case http.StatusNotFound:
			msg := fmt.Sprintf("delete stream=%s not found (skip)", streamName)
			if notFoundOK {
				res.warnings = append(res.warnings, msg)
			} else if cfg.Strict {
				res.errors = append(res.errors, msg)
			} else {
				res.warnings = append(res.warnings, msg)
			}
			return
		case http.StatusConflict:
			if attempt < cfg.StreamDeleteRetries {
				fmt.Printf("[resource] delete stream=%s conflict retry=%d/%d\n", streamName, attempt, cfg.StreamDeleteRetries)
				if interval > 0 {
					time.Sleep(interval)
				}
				continue
			}
			res.errors = append(res.errors, fmt.Sprintf("delete stream %s failed after retries: HTTP %d: %s", streamName, status, trimBody(body)))
			return
		default:
			res.errors = append(res.errors, fmt.Sprintf("delete stream %s failed: HTTP %d: %s", streamName, status, trimBody(body)))
			return
		}
	}
}

func createStream(client *apiClient, cfg *controlConfig, spec streamSpec, res *opResult) {
	body := map[string]any{
		"name":     spec.Name,
		"revision": 1,
		"type":     "mqtt",
		"props": map[string]any{
			"broker_url": cfg.BrokerURL,
			"topic":      spec.Topic,
			"qos":        cfg.QoS,
		},
		"schema": map[string]any{
			"type": "json",
			"props": map[string]any{
				"columns": buildColumns(cfg.Columns),
			},
		},
		"decoder": map[string]any{
			"type":  "json",
			"props": map[string]any{},
		},
		"shared": false,
	}

	status, raw, err := callWithRetry(cfg, func() (int, []byte, error) {
		return client.doJSON(http.MethodPost, "/streams", nil, body)
	})
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("create stream %s request failed: %v", spec.Name, err))
		return
	}

	switch status {
	case http.StatusOK, http.StatusCreated:
		fmt.Printf("[resource] create stream=%s status=%d\n", spec.Name, status)
		return
	case http.StatusConflict:
		fmt.Printf("[resource] create stream=%s exists, recreate\n", spec.Name)
		deleteStream(client, cfg, spec.Name, res, true)
		status2, raw2, err2 := callWithRetry(cfg, func() (int, []byte, error) {
			return client.doJSON(http.MethodPost, "/streams", nil, body)
		})
		if err2 != nil {
			res.errors = append(res.errors, fmt.Sprintf("recreate stream %s request failed: %v", spec.Name, err2))
			return
		}
		if status2 == http.StatusOK || status2 == http.StatusCreated {
			fmt.Printf("[resource] recreate stream=%s status=%d\n", spec.Name, status2)
			return
		}
		res.errors = append(res.errors, fmt.Sprintf("recreate stream %s failed: HTTP %d: %s", spec.Name, status2, trimBody(raw2)))
		return
	default:
		res.errors = append(res.errors, fmt.Sprintf("create stream %s failed: HTTP %d: %s", spec.Name, status, trimBody(raw)))
		return
	}
}

func createPipeline(client *apiClient, cfg *controlConfig, spec pipelineSpec, res *opResult) {
	sql := buildSelectSQLByMode(spec.StreamName, cfg.Columns, cfg.SQLMode)
	body := map[string]any{
		"id":               spec.ID,
		"revision":         1,
		"sql":              sql,
		"flow_instance_id": spec.FlowInstanceID,
		"sinks": []any{
			map[string]any{
				"type":  "nop",
				"props": map[string]any{"log": false},
				"common_sink_props": map[string]any{
					"batch_duration": 100,
					"batch_count":    50,
				},
				"encoder": map[string]any{
					"type":  "json",
					"props": map[string]any{},
				},
			},
		},
		"options": map[string]any{
			"eventtime": map[string]any{
				"enabled": false,
			},
		},
	}

	status, raw, err := callWithRetry(cfg, func() (int, []byte, error) {
		return client.doJSON(http.MethodPost, "/pipelines", nil, body)
	})
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("create pipeline %s request failed: %v", spec.ID, err))
		return
	}
	switch status {
	case http.StatusOK, http.StatusCreated:
		fmt.Printf("[resource] create pipeline=%s status=%d\n", spec.ID, status)
		return
	case http.StatusConflict:
		fmt.Printf("[resource] create pipeline=%s exists, recreate\n", spec.ID)
		deletePipeline(client, cfg, spec.ID, res, true)
		status2, raw2, err2 := callWithRetry(cfg, func() (int, []byte, error) {
			return client.doJSON(http.MethodPost, "/pipelines", nil, body)
		})
		if err2 != nil {
			res.errors = append(res.errors, fmt.Sprintf("recreate pipeline %s request failed: %v", spec.ID, err2))
			return
		}
		if status2 == http.StatusOK || status2 == http.StatusCreated {
			fmt.Printf("[resource] recreate pipeline=%s status=%d\n", spec.ID, status2)
			return
		}
		res.errors = append(res.errors, fmt.Sprintf("recreate pipeline %s failed: HTTP %d: %s", spec.ID, status2, trimBody(raw2)))
		return
	default:
		res.errors = append(res.errors, fmt.Sprintf("create pipeline %s failed: HTTP %d: %s", spec.ID, status, trimBody(raw)))
	}
}

func verifyPresence(client *apiClient, cfg *controlConfig, streams []streamSpec, pipelines []pipelineSpec, res *opResult) {
	streamSet, err := listStreams(client, cfg)
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("verify streams failed: %v", err))
	} else {
		for _, stream := range streams {
			if _, ok := streamSet[stream.Name]; !ok {
				msg := fmt.Sprintf("stream missing after provision: %s", stream.Name)
				if cfg.Strict {
					res.errors = append(res.errors, msg)
				} else {
					res.warnings = append(res.warnings, msg)
				}
			}
		}
	}

	pipelineSet, err := listPipelines(client, cfg)
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("verify pipelines failed: %v", err))
	} else {
		for _, pipeline := range pipelines {
			if _, ok := pipelineSet[pipeline.ID]; !ok {
				msg := fmt.Sprintf("pipeline missing after provision: %s", pipeline.ID)
				if cfg.Strict {
					res.errors = append(res.errors, msg)
				} else {
					res.warnings = append(res.warnings, msg)
				}
			}
		}
	}
}

func verifyAbsence(client *apiClient, cfg *controlConfig, streams []string, pipelines []string, res *opResult) {
	streamSet, err := listStreams(client, cfg)
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("verify streams failed: %v", err))
	} else {
		for _, stream := range streams {
			if _, ok := streamSet[stream]; ok {
				msg := fmt.Sprintf("stream still exists after delete: %s", stream)
				if cfg.Strict {
					res.errors = append(res.errors, msg)
				} else {
					res.warnings = append(res.warnings, msg)
				}
			}
		}
	}

	pipelineSet, err := listPipelines(client, cfg)
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf("verify pipelines failed: %v", err))
	} else {
		for _, pipeline := range pipelines {
			if _, ok := pipelineSet[pipeline]; ok {
				msg := fmt.Sprintf("pipeline still exists after delete: %s", pipeline)
				if cfg.Strict {
					res.errors = append(res.errors, msg)
				} else {
					res.warnings = append(res.warnings, msg)
				}
			}
		}
	}
}

func listStreams(client *apiClient, cfg *controlConfig) (map[string]struct{}, error) {
	return listFieldSet(client, cfg, "/streams", "name")
}

func listPipelines(client *apiClient, cfg *controlConfig) (map[string]struct{}, error) {
	return listFieldSet(client, cfg, "/pipelines", "id")
}

func listFieldSet(client *apiClient, cfg *controlConfig, path, field string) (map[string]struct{}, error) {
	status, body, err := callWithRetry(cfg, func() (int, []byte, error) {
		return client.doJSON(http.MethodGet, path, nil, nil)
	})
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s failed: HTTP %d: %s", path, status, trimBody(body))
	}

	items, err := parseObjectList(body)
	if err != nil {
		return nil, fmt.Errorf("parse %s failed: %w", path, err)
	}

	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		if val, ok := item[field].(string); ok && strings.TrimSpace(val) != "" {
			set[val] = struct{}{}
		}
	}
	return set, nil
}

func parseObjectList(raw []byte) ([]map[string]any, error) {
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	extract := func(arr []any) []map[string]any {
		out := make([]map[string]any, 0, len(arr))
		for _, item := range arr {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}

	switch v := data.(type) {
	case []any:
		return extract(v), nil
	case map[string]any:
		for _, key := range []string{"items", "data", "list"} {
			if arr, ok := v[key].([]any); ok {
				return extract(arr), nil
			}
		}
		return nil, errors.New("unsupported object shape")
	default:
		return nil, errors.New("unsupported json shape")
	}
}

func finalizeResult(command string, strict bool, res *opResult) error {
	for _, warning := range res.warnings {
		fmt.Printf("[resource][warn] %s\n", warning)
	}
	for _, msg := range res.errors {
		fmt.Printf("[resource][error] %s\n", msg)
	}

	if strict && len(res.errors) > 0 {
		return fmt.Errorf("%s finished with %d error(s)", command, len(res.errors))
	}
	if !strict && len(res.errors) > 0 {
		fmt.Printf("[resource][warn] non-strict mode ignores %d error(s)\n", len(res.errors))
	}
	fmt.Printf("[resource] %s done strict=%t warnings=%d errors=%d\n", command, strict, len(res.warnings), len(res.errors))
	return nil
}

func buildColumns(count int) []map[string]string {
	cols := make([]map[string]string, 0, count)
	for i := 1; i <= count; i++ {
		cols = append(cols, map[string]string{
			"name":      fmt.Sprintf("a%d", i),
			"data_type": "string",
		})
	}
	return cols
}

func buildSelectSQLByMode(streamName string, columns int, mode string) string {
	if mode == "star" {
		return fmt.Sprintf("select * from %s", streamName)
	}
	names := make([]string, 0, columns)
	for i := 1; i <= columns; i++ {
		names = append(names, fmt.Sprintf("a%d", i))
	}
	return fmt.Sprintf("select %s from %s", strings.Join(names, ","), streamName)
}

func trimBody(raw []byte) string {
	if len(raw) == 0 {
		return "<empty>"
	}
	s := strings.TrimSpace(string(raw))
	if len(s) > 300 {
		return s[:300] + "...(truncated)"
	}
	return s
}

func generatePayloadCases(columnCount, cases, strLen int) ([][]byte, error) {
	keys := make([]string, columnCount)
	for idx := range keys {
		keys[idx] = fmt.Sprintf("a%d", idx+1)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	out := make([][]byte, 0, cases)
	for i := 0; i < cases; i++ {
		obj := make(map[string]string, columnCount)
		for _, key := range keys {
			obj[key] = randomString(rng, strLen)
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("json marshal payload failed: %w", err)
		}
		out = append(out, raw)
	}
	return out, nil
}

func randomString(rng *rand.Rand, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = randomAlphabet[rng.Intn(len(randomAlphabet))]
	}
	return string(buf)
}

func runSingle(cfg *runConfig, payloads [][]byte, durationSec, rateCritical, rateBest int) error {
	start := time.Now().UTC()
	fmt.Printf(
		"[workload] start=%s duration=%ds rate_critical=%d rate_best=%d\n",
		start.Format(time.RFC3339),
		durationSec,
		rateCritical,
		rateBest,
	)

	resultsCh := make(chan publisherResult, 2)
	duration := time.Duration(durationSec) * time.Second

	go publishLoad(
		cfg,
		"critical",
		cfg.TopicCritical,
		rateCritical,
		duration,
		payloads,
		resultsCh,
	)
	go publishLoad(
		cfg,
		"best",
		cfg.TopicBest,
		rateBest,
		duration,
		payloads,
		resultsCh,
	)

	results := make([]publisherResult, 0, 2)
	for i := 0; i < 2; i++ {
		results = append(results, <-resultsCh)
	}

	end := time.Now().UTC()
	fmt.Printf("[workload] end=%s\n", end.Format(time.RFC3339))

	var finalErr error
	for _, result := range results {
		effectiveRate := 0.0
		if result.Duration > 0 {
			effectiveRate = float64(result.Sent) / result.Duration.Seconds()
		}
		fmt.Printf(
			"[workload] publisher=%s topic=%s target_rate=%d sent=%d effective_rate=%.2f err=%v\n",
			result.Label,
			result.Topic,
			result.Rate,
			result.Sent,
			effectiveRate,
			result.Err,
		)
		if result.Err != nil && finalErr == nil {
			finalErr = fmt.Errorf("%s publisher failed: %w", result.Label, result.Err)
		}
	}
	return finalErr
}

func publishLoad(
	cfg *runConfig,
	label string,
	topic string,
	ratePerSec int,
	duration time.Duration,
	payloads [][]byte,
	out chan<- publisherResult,
) {
	start := time.Now()
	res := publisherResult{
		Label: label,
		Topic: topic,
		Rate:  ratePerSec,
	}
	defer func() {
		res.Duration = time.Since(start)
		out <- res
	}()

	clientID := fmt.Sprintf("%s-%s-%d-%d", cfg.ClientPrefix, label, os.Getpid(), time.Now().UnixNano())
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second).
		SetKeepAlive(60 * time.Second).
		SetAutoReconnect(false).
		SetCleanSession(true).
		SetOrderMatters(false)
	client := mqtt.NewClient(opts)

	connectToken := client.Connect()
	if !connectToken.WaitTimeout(15 * time.Second) {
		res.Err = errors.New("mqtt connect timeout")
		return
	}
	if connectToken.Error() != nil {
		res.Err = connectToken.Error()
		return
	}
	defer client.Disconnect(250)

	if duration <= 0 || ratePerSec <= 0 {
		if duration > 0 {
			time.Sleep(duration)
		}
		return
	}

	interval := time.Second / time.Duration(ratePerSec)
	deadline := time.Now().Add(duration)
	nextSend := time.Now()
	idx := 0
	qos := byte(cfg.QoS)

	for time.Now().Before(deadline) {
		payload := payloads[idx%len(payloads)]
		idx++

		token := client.Publish(topic, qos, false, payload)
		if !token.WaitTimeout(30 * time.Second) {
			res.Err = errors.New("mqtt publish timeout")
			return
		}
		if token.Error() != nil {
			res.Err = token.Error()
			return
		}
		res.Sent++

		nextSend = nextSend.Add(interval)
		sleepFor := time.Until(nextSend)
		if sleepFor > 0 {
			time.Sleep(sleepFor)
			continue
		}
		nextSend = time.Now()
	}
}
