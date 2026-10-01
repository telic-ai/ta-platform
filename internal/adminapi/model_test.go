package adminapi

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

func TestValidEmail(t *testing.T) {
	for in, want := range map[string]string{"a@b.test": "a@b.test", " A@B.Test ": "a@b.test"} {
		if got, err := ValidEmail(in); err != nil || got != want {
			t.Errorf("ValidEmail(%q) = %q, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "nope", "Ada <a@b.test>", "a@b.test, c@d.test"} {
		if _, err := ValidEmail(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidEmail(%q) err = %v", in, err)
		}
	}
}

func TestValidName(t *testing.T) {
	if got, err := validName("n", "  x "); err != nil || got != "x" {
		t.Fatalf("got %q %v", got, err)
	}
	for _, in := range []string{"", "   ", string(make([]byte, maxNameLength+1))} {
		if _, err := validName("n", in); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
	if _, err := validText("t", string(make([]byte, maxTextLength+1))); err == nil {
		t.Error("validText accepted oversize text")
	}
}

func TestInterviewAndTaskStatuses(t *testing.T) {
	for _, s := range []string{"scheduled", "in_progress", "completed", "cancelled"} {
		if !ValidInterviewStatus(s) {
			t.Errorf("%s invalid", s)
		}
	}
	if ValidInterviewStatus("purged") {
		t.Error("purged valid")
	}
	if !TerminalInterviewStatus("completed") || !TerminalInterviewStatus("cancelled") || TerminalInterviewStatus("in_progress") {
		t.Error("terminal statuses wrong")
	}
	if !ValidTaskStatus("open") || !ValidTaskStatus("done") || ValidTaskStatus("closed") {
		t.Error("task statuses wrong")
	}
}

func TestValidateScoreDecision(t *testing.T) {
	v := 1.0
	ok := []struct {
		status string
		value  *float64
	}{{"human_approved", nil}, {"human_rejected", nil}, {"human_adjusted", &v}}
	for _, tc := range ok {
		if err := ValidateScoreDecision(tc.status, tc.value); err != nil {
			t.Errorf("%s: %v", tc.status, err)
		}
	}
	bad := []struct {
		status string
		value  *float64
	}{{"proposed", nil}, {"ai_approved", nil}, {"", nil}, {"human_adjusted", nil}, {"human_approved", &v}, {"human_rejected", &v}, {"human_other", nil}}
	for _, tc := range bad {
		if err := ValidateScoreDecision(tc.status, tc.value); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s/%v accepted", tc.status, tc.value)
		}
	}
}

func TestCanManageRole(t *testing.T) {
	cases := []struct {
		actor, target rbac.Role
		want          bool
	}{
		{rbac.RoleOwner, rbac.RoleOwner, true},
		{rbac.RoleOwner, rbac.RoleViewer, true},
		{rbac.RoleAdmin, rbac.RoleOwner, false},
		{rbac.RoleAdmin, rbac.RoleAdmin, true},
		{rbac.RoleAdmin, rbac.RoleInterviewer, true},
		{rbac.RoleRecruiter, rbac.RoleViewer, false},
		{rbac.RoleViewer, rbac.RoleViewer, false},
	}
	for _, tc := range cases {
		if got := CanManageRole(tc.actor, tc.target); got != tc.want {
			t.Errorf("CanManageRole(%s, %s) = %v", tc.actor, tc.target, got)
		}
	}
}

func TestMergePolicies(t *testing.T) {
	merged := MergePolicies([]domain.Policy{{Key: "replay", Enabled: false}, {Key: "retired_flag", Enabled: true}})
	if len(merged) != len(PolicyKeys()) {
		t.Fatalf("merged = %+v", merged)
	}
	for i, p := range merged {
		if p.Key != PolicyKeys()[i] {
			t.Fatalf("order: %+v", merged)
		}
		if want := p.Key != "replay"; p.Enabled != want {
			t.Errorf("%s enabled = %v", p.Key, p.Enabled)
		}
	}
	for _, key := range PolicyKeys() {
		if _, known := PolicyDefault(key); !known {
			t.Errorf("%s has no default", key)
		}
	}
	if _, known := PolicyDefault("nope"); known {
		t.Error("unknown key has a default")
	}
}

func TestOptionalDistinguishesAbsentFromNull(t *testing.T) {
	var body struct {
		A optional[uuid.UUID] `json:"a"`
		B optional[uuid.UUID] `json:"b"`
		C optional[uuid.UUID] `json:"c"`
	}
	id := uuid.New()
	if err := json.Unmarshal([]byte(`{"a":null,"b":"`+id.String()+`"}`), &body); err != nil {
		t.Fatal(err)
	}
	if !body.A.Set || body.A.Value != nil || !body.B.Set || *body.B.Value != id || body.C.Set {
		t.Fatalf("body = %+v", body)
	}
	if err := json.Unmarshal([]byte(`{"a":"nope"}`), &body); err == nil {
		t.Fatal("accepted invalid UUID")
	}
}
