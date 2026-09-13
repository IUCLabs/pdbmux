package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

const (
	numNodesMBean = "puppetlabs.puppetdb.population:name=num-nodes"
	numNodesPath  = metricsPrefix + "v2/read/" + numNodesMBean
	// What pypuppetdb actually sends: quote() percent-encodes ':' and '='.
	numNodesEscaped = metricsPrefix + "v2/read/puppetlabs.puppetdb.population%3Aname%3Dnum-nodes"
)

// jolokiaRead wraps an MBean value in the envelope PuppetDB's Jolokia returns.
func jolokiaRead(mbean, value string, timestamp int) string {
	return `{"request":{"mbean":"` + mbean + `","type":"read"},` +
		`"value":` + value + `,"timestamp":` + strconv.Itoa(timestamp) + `,"status":200}`
}

func metricValue(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var env map[string]json.RawMessage
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal envelope %s: %v", body, err)
	}
	var val map[string]any
	if err := json.Unmarshal(env["value"], &val); err != nil {
		t.Fatalf("unmarshal value %s: %v", env["value"], err)
	}
	return val
}

func TestMetrics_ReadSumsPopulationCounts(t *testing.T) {
	// Puppetboard's landing page and radiator read num-nodes when
	// DEFAULT_ENVIRONMENT is '*'; each backend only knows its own nodes.
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":90}`, 1000)
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":53}`, 2000)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), numNodesEscaped)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := metricValue(t, rec.Body.Bytes())["Value"]; got != float64(143) {
		t.Errorf("Value = %v, want 143", got)
	}
}

func TestMetrics_EscapedMBeanNameSurvives(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":1}`, 1)
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":1}`, 1)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	metaGet(t, srv.Handler(), numNodesEscaped)
	if !a.sawRawPath(numNodesEscaped) {
		t.Errorf("backend saw %v, want the percent-encoded path %q", a.rawPaths, numNodesEscaped)
	}
}

func TestMetrics_EnvelopeKeepsNewestTimestamp(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":1}`, 1000)
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":1}`, 2000)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	var env map[string]any
	if err := json.Unmarshal(metaGet(t, srv.Handler(), numNodesEscaped).Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env["timestamp"] != float64(2000) {
		t.Errorf("timestamp = %v, want 2000", env["timestamp"])
	}
	if env["status"] != float64(200) {
		t.Errorf("status = %v, want 200", env["status"])
	}
}

func TestMetrics_PerAttributeRules(t *testing.T) {
	const mbean = "puppetlabs.puppetdb.mq:name=global.processing-time"
	path := metricsPrefix + "v2/read/" + mbean
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = jolokiaRead(mbean,
		`{"Count":10,"Min":2,"Max":9,"Mean":4,"StdDev":1,"50thPercentile":3,"MeanRate":1.5}`, 1)
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[path] = jolokiaRead(mbean,
		`{"Count":6,"Min":1,"Max":20,"Mean":6,"StdDev":3,"50thPercentile":5,"MeanRate":0.5}`, 1)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	got := metricValue(t, metaGet(t, srv.Handler(), path).Body.Bytes())
	want := map[string]any{
		"Count":          float64(16), // counts add
		"Min":            float64(1),  // a bound stays a bound
		"Max":            float64(20),
		"Mean":           float64(5), // distribution stats average
		"StdDev":         float64(2),
		"50thPercentile": float64(4),
		"MeanRate":       float64(2), // throughput adds
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged value = %v, want %v", got, want)
	}
}

func TestMetrics_ScalarReadUsesURLAttributeName(t *testing.T) {
	// /metrics/v2/read/<mbean>/<attribute> answers with a bare number, so the
	// rule has to come from the URL rather than an object key.
	const mbean = "puppetlabs.puppetdb.population:name=num-resources"
	sumPath := metricsPrefix + "v2/read/" + mbean + "/Value"
	maxPath := metricsPrefix + "v2/read/" + mbean + "/Max"

	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[sumPath] = jolokiaRead(mbean, `1000`, 1)
	a.bodies[maxPath] = jolokiaRead(mbean, `1000`, 1)
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[sumPath] = jolokiaRead(mbean, `234`, 1)
	b.bodies[maxPath] = jolokiaRead(mbean, `234`, 1)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	for _, c := range []struct {
		path string
		want float64
	}{{sumPath, 1234}, {maxPath, 1000}} {
		var env map[string]any
		if err := json.Unmarshal(metaGet(t, srv.Handler(), c.path).Body.Bytes(), &env); err != nil {
			t.Fatal(err)
		}
		if env["value"] != c.want {
			t.Errorf("%s value = %v, want %v", c.path, env["value"], c.want)
		}
	}
}

func TestMetrics_ListUnionsDomains(t *testing.T) {
	// Puppetboard's /metrics page calls metric() with no name, which is a
	// Jolokia list; a backend-local MBean must not vanish from the browse tree.
	const path = metricsPrefix + "v2/list"
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = `{"value":{"java.lang":{"type=Memory":{"attr":{"HeapMemoryUsage":{"rw":false}}}}},"status":200,"timestamp":1}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[path] = `{"value":{"puppetlabs.puppetdb.population":{"name=num-nodes":{"attr":{"Value":{"rw":false}}}}},"status":200,"timestamp":1}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	got := metricValue(t, metaGet(t, srv.Handler(), path).Body.Bytes())
	if _, ok := got["java.lang"]; !ok {
		t.Errorf("java.lang missing from merged list: %v", got)
	}
	if _, ok := got["puppetlabs.puppetdb.population"]; !ok {
		t.Errorf("puppetlabs.puppetdb.population missing from merged list: %v", got)
	}
}

func TestMetrics_V1BareObjectMerged(t *testing.T) {
	// metrics/v1/mbeans has no Jolokia envelope; the whole body is the value.
	const path = metricsPrefix + "v1/mbeans/" + numNodesMBean
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = `{"Value":90}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[path] = `{"Value":53}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	var got map[string]any
	if err := json.Unmarshal(metaGet(t, srv.Handler(), path).Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["Value"] != float64(143) {
		t.Errorf("Value = %v, want 143", got["Value"])
	}
}

func TestMetrics_MissingMBeanKeepsUpstreamError(t *testing.T) {
	// Jolokia reports a bad MBean as a 200 with an error envelope, which
	// pypuppetdb turns into DoesNotComputeError; the client must still see it.
	const path = metricsPrefix + "v2/read/nope:name=nothing"
	errEnv := `{"request":{"mbean":"nope:name=nothing"},"error_type":"javax.management.InstanceNotFoundException","error":"nope:name=nothing is not registered","status":404}`
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = errEnv
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[path] = errEnv
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), path)
	var env map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env["error"] == nil {
		t.Errorf("expected the upstream Jolokia error to be replayed, got %s", rec.Body.String())
	}
}

func TestMetrics_ErroringBackendIgnoredWhenAnotherAnswers(t *testing.T) {
	const path = metricsPrefix + "v2/read/" + numNodesMBean
	a := newFakeBackend(t, `[]`, `[]`)
	a.bodies[path] = `{"request":{},"error":"boom","status":500}`
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[path] = jolokiaRead(numNodesMBean, `{"Value":53}`, 1)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if got := metricValue(t, metaGet(t, srv.Handler(), path).Body.Bytes())["Value"]; got != float64(53) {
		t.Errorf("Value = %v, want 53 from the backend that answered", got)
	}
}

func TestMetrics_OneBackendDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.fail = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.bodies[numNodesPath] = jolokiaRead(numNodesMBean, `{"Value":53}`, 1)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), numNodesEscaped)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 serving the survivor, got %d", rec.Code)
	}
	if got := metricValue(t, rec.Body.Bytes())["Value"]; got != float64(53) {
		t.Errorf("Value = %v, want 53", got)
	}
}

func TestMetrics_AllBackendsDown(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	a.dead = true
	b := newFakeBackend(t, `[]`, `[]`)
	b.dead = true
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := metaGet(t, srv.Handler(), numNodesEscaped); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502", rec.Code)
	}
}

// /metrics/v1/mbeans is a unanimous 404 on this estate and /metrics/v2 a
// unanimous 403; both used to come back as 502.
func TestMetrics_ReplaysUnanimousUpstreamStatus(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			a := newFakeBackend(t, `[]`, `[]`)
			b := newFakeBackend(t, `[]`, `[]`)
			a.reject, a.rejectBody = status, "Forbidden request: /metrics/v2/list (method :get)."
			b.reject, b.rejectBody = status, "Forbidden request: /metrics/v2/list (method :get)."
			srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

			rec := metaGet(t, srv.Handler(), numNodesEscaped)
			if rec.Code != status {
				t.Fatalf("status %d, want the upstream %d: %s", rec.Code, status, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "Forbidden request") {
				t.Errorf("body = %q, want the upstream explanation", rec.Body.String())
			}
		})
	}
}

func TestMetrics_DisagreeingStatusesStay502(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusNotFound, "gone"
	b.reject, b.rejectBody = http.StatusForbidden, "denied"
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	if rec := metaGet(t, srv.Handler(), numNodesEscaped); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502 when backends disagree", rec.Code)
	}
}

// One backend refusing is not the estate's answer, so the other still serves.
func TestMetrics_OneRefusalStillServesTheOther(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	a.reject, a.rejectBody = http.StatusForbidden, "denied"
	b.bodies[numNodesPath] = `{"request":{},"value":{"Value":7},"status":200}`
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := metaGet(t, srv.Handler(), numNodesEscaped)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want the surviving backend's 200: %s", rec.Code, rec.Body.String())
	}
	if got := metricValue(t, rec.Body.Bytes())["Value"]; got != float64(7) {
		t.Errorf("Value = %v, want 7", got)
	}
}

func TestMetrics_RejectsNonGET(t *testing.T) {
	a := newFakeBackend(t, `[]`, `[]`)
	b := newFakeBackend(t, `[]`, `[]`)
	srv := newTestServer(testConfig(a.srv.URL, b.srv.URL, mergeStatic))

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, numNodesEscaped, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status %d, want 405", rec.Code)
	}
}

func TestMergeMetricValue_NonNumericKeepsFirst(t *testing.T) {
	a := map[string]any{"Name": "pdb-a", "Enabled": true}
	b := map[string]any{"Name": "pdb-b", "Enabled": false}
	got, ok := mergeMetricValue([]any{a, b}, "").(map[string]any)
	if !ok {
		t.Fatalf("expected an object, got %T", got)
	}
	if got["Name"] != "pdb-a" || got["Enabled"] != true {
		t.Errorf("merged = %v, want the first backend's strings and booleans", got)
	}
}

func TestMergeRuleFor(t *testing.T) {
	cases := map[string]mergeRule{
		"Count":          ruleSum,
		"Value":          ruleSum,
		"MeanRate":       ruleSum,
		"queue-depth":    ruleSum,
		"min":            ruleMin,
		"Max":            ruleMax,
		"Uptime":         ruleMax,
		"StartTime":      ruleMax,
		"Mean":           ruleMean,
		"StdDev":         ruleMean,
		"99thPercentile": ruleMean,
	}
	for attr, want := range cases {
		if got := mergeRuleFor(attr); got != want {
			t.Errorf("mergeRuleFor(%q) = %v, want %v", attr, got, want)
		}
	}
}
