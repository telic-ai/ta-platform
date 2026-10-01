package adminapi

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/telic-ai/ta-platform/internal/domain"
	"github.com/telic-ai/ta-platform/internal/rbac"
)

// memoryStore is a tenant-scoped in-memory Store. Records are keyed by
// (company, id), so a lookup with the wrong company misses just as the
// Postgres store's does.
type memoryStore struct {
	mu         sync.Mutex
	companies  map[uuid.UUID]domain.Company
	users      map[key]domain.User
	interviews map[key]domain.Interview
	tasks      map[key]domain.Task
	invites    []domain.Invite
	scores     map[key]domain.Score
	policies   map[key]domain.Policy
	err        error
	// companiesSeen records the company of every call, so tests can assert
	// scoping.
	companiesSeen []uuid.UUID
}

type key struct{ company, id uuid.UUID }

type policyKey = key

func newMemoryStore() *memoryStore {
	return &memoryStore{
		companies: map[uuid.UUID]domain.Company{}, users: map[key]domain.User{},
		interviews: map[key]domain.Interview{}, tasks: map[key]domain.Task{},
		scores: map[key]domain.Score{}, policies: map[key]domain.Policy{},
	}
}

func (s *memoryStore) seen(companyID uuid.UUID) error {
	s.companiesSeen = append(s.companiesSeen, companyID)
	return s.err
}

func (s *memoryStore) UserRole(_ context.Context, companyID, userID uuid.UUID) (rbac.Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[key{companyID, userID}]
	if !ok {
		return "", rbac.ErrUserNotFound
	}
	return rbac.Role(u.Role), nil
}

func (s *memoryStore) GetCompany(_ context.Context, companyID uuid.UUID) (domain.Company, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Company{}, err
	}
	c, ok := s.companies[companyID]
	if !ok {
		return domain.Company{}, ErrNotFound
	}
	return c, nil
}

func (s *memoryStore) UpdateCompany(_ context.Context, companyID uuid.UUID, patch CompanyPatch) (domain.Company, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Company{}, err
	}
	c, ok := s.companies[companyID]
	if !ok {
		return domain.Company{}, ErrNotFound
	}
	if patch.Name != nil {
		c.Name = *patch.Name
	}
	s.companies[companyID] = c
	return c, nil
}

func (s *memoryStore) ListUsers(_ context.Context, companyID uuid.UUID) ([]domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return nil, err
	}
	var out []domain.User
	for k, u := range s.users {
		if k.company == companyID {
			out = append(out, u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Email < out[j].Email })
	return out, nil
}

func (s *memoryStore) GetUser(_ context.Context, companyID, userID uuid.UUID) (domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.User{}, err
	}
	u, ok := s.users[key{companyID, userID}]
	if !ok {
		return domain.User{}, ErrNotFound
	}
	return u, nil
}

func (s *memoryStore) CreateUser(_ context.Context, user domain.User) (domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(user.CompanyID); err != nil {
		return domain.User{}, err
	}
	for k, u := range s.users {
		if k.company == user.CompanyID && u.Email == user.Email {
			return domain.User{}, ErrConflict
		}
	}
	s.users[key{user.CompanyID, user.ID}] = user
	return user, nil
}

func (s *memoryStore) UpdateUser(_ context.Context, companyID, userID uuid.UUID, patch UserPatch) (domain.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.User{}, err
	}
	u, ok := s.users[key{companyID, userID}]
	if !ok {
		return domain.User{}, ErrNotFound
	}
	if patch.DisplayName != nil {
		u.DisplayName = *patch.DisplayName
	}
	if patch.Role != nil {
		u.Role = string(*patch.Role)
	}
	s.users[key{companyID, userID}] = u
	return u, nil
}

func (s *memoryStore) DeleteUser(_ context.Context, companyID, userID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return err
	}
	if _, ok := s.users[key{companyID, userID}]; !ok {
		return ErrNotFound
	}
	delete(s.users, key{companyID, userID})
	return nil
}

func (s *memoryStore) ListInterviews(_ context.Context, companyID uuid.UUID, filter InterviewFilter) ([]domain.Interview, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return nil, err
	}
	var out []domain.Interview
	for k, i := range s.interviews {
		if k.company == companyID && (filter.Status == "" || i.Status == filter.Status) {
			out = append(out, i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CandidateName < out[b].CandidateName })
	if len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *memoryStore) GetInterview(_ context.Context, companyID, interviewID uuid.UUID) (domain.Interview, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Interview{}, err
	}
	i, ok := s.interviews[key{companyID, interviewID}]
	if !ok {
		return domain.Interview{}, ErrNotFound
	}
	return i, nil
}

func (s *memoryStore) CreateInterview(_ context.Context, interview domain.Interview) (domain.Interview, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(interview.CompanyID); err != nil {
		return domain.Interview{}, err
	}
	s.interviews[key{interview.CompanyID, interview.ID}] = interview
	return interview, nil
}

func (s *memoryStore) UpdateInterview(_ context.Context, companyID, interviewID uuid.UUID, patch InterviewPatch) (domain.Interview, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Interview{}, err
	}
	i, ok := s.interviews[key{companyID, interviewID}]
	if !ok {
		return domain.Interview{}, ErrNotFound
	}
	if patch.CandidateName != nil {
		i.CandidateName = *patch.CandidateName
	}
	if patch.CandidateEmail != nil {
		i.CandidateEmail = *patch.CandidateEmail
	}
	if patch.Status != nil {
		i.Status = *patch.Status
	}
	if patch.ScheduledAt != nil {
		i.ScheduledAt = patch.ScheduledAt
	}
	if patch.TerminalAt != nil && i.TerminalAt == nil {
		i.TerminalAt = patch.TerminalAt
	}
	s.interviews[key{companyID, interviewID}] = i
	return i, nil
}

func (s *memoryStore) RequestErasure(_ context.Context, companyID, interviewID uuid.UUID, at time.Time) (domain.Interview, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Interview{}, err
	}
	i, ok := s.interviews[key{companyID, interviewID}]
	if !ok {
		return domain.Interview{}, ErrNotFound
	}
	if i.EraseRequestedAt == nil {
		i.EraseRequestedAt = &at
	}
	s.interviews[key{companyID, interviewID}] = i
	return i, nil
}

func (s *memoryStore) ListTasks(_ context.Context, companyID uuid.UUID, filter TaskFilter) ([]domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return nil, err
	}
	var out []domain.Task
	for k, t := range s.tasks {
		if k.company != companyID ||
			(filter.Status != "" && t.Status != filter.Status) ||
			(filter.InterviewID != nil && (t.InterviewID == nil || *t.InterviewID != *filter.InterviewID)) ||
			(filter.AssigneeID != nil && (t.AssigneeID == nil || *t.AssigneeID != *filter.AssigneeID)) {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Title < out[b].Title })
	return out, nil
}

func (s *memoryStore) GetTask(_ context.Context, companyID, taskID uuid.UUID) (domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Task{}, err
	}
	t, ok := s.tasks[key{companyID, taskID}]
	if !ok {
		return domain.Task{}, ErrNotFound
	}
	return t, nil
}

func (s *memoryStore) checkRefs(companyID uuid.UUID, interviewID, assigneeID *uuid.UUID) error {
	if interviewID != nil {
		if _, ok := s.interviews[key{companyID, *interviewID}]; !ok {
			return ErrInvalidReference
		}
	}
	if assigneeID != nil && *assigneeID != uuid.Nil {
		if _, ok := s.users[key{companyID, *assigneeID}]; !ok {
			return ErrInvalidReference
		}
	}
	return nil
}

func (s *memoryStore) CreateTask(_ context.Context, task domain.Task) (domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(task.CompanyID); err != nil {
		return domain.Task{}, err
	}
	if err := s.checkRefs(task.CompanyID, task.InterviewID, task.AssigneeID); err != nil {
		return domain.Task{}, err
	}
	s.tasks[key{task.CompanyID, task.ID}] = task
	return task, nil
}

func (s *memoryStore) UpdateTask(_ context.Context, companyID, taskID uuid.UUID, patch TaskPatch) (domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Task{}, err
	}
	t, ok := s.tasks[key{companyID, taskID}]
	if !ok {
		return domain.Task{}, ErrNotFound
	}
	if err := s.checkRefs(companyID, nil, patch.AssigneeID); err != nil {
		return domain.Task{}, err
	}
	if patch.Title != nil {
		t.Title = *patch.Title
	}
	if patch.Description != nil {
		t.Description = *patch.Description
	}
	if patch.Status != nil {
		t.Status = *patch.Status
	}
	if patch.AssigneeID != nil {
		if *patch.AssigneeID == uuid.Nil {
			t.AssigneeID = nil
		} else {
			assignee := *patch.AssigneeID
			t.AssigneeID = &assignee
		}
	}
	if patch.DueAt != nil {
		t.DueAt = patch.DueAt
	}
	if patch.CompletedAt != nil {
		t.CompletedAt = patch.CompletedAt
	}
	s.tasks[key{companyID, taskID}] = t
	return t, nil
}

func (s *memoryStore) DeleteTask(_ context.Context, companyID, taskID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return err
	}
	if _, ok := s.tasks[key{companyID, taskID}]; !ok {
		return ErrNotFound
	}
	delete(s.tasks, key{companyID, taskID})
	return nil
}

func (s *memoryStore) CreateInvite(_ context.Context, invite domain.Invite) (domain.Invite, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(invite.CompanyID); err != nil {
		return domain.Invite{}, err
	}
	if err := s.checkRefs(invite.CompanyID, invite.InterviewID, nil); err != nil {
		return domain.Invite{}, err
	}
	s.invites = append(s.invites, invite)
	return invite, nil
}

func (s *memoryStore) ListScores(_ context.Context, companyID, interviewID uuid.UUID) ([]domain.Score, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return nil, err
	}
	var out []domain.Score
	for k, score := range s.scores {
		if k.company == companyID && score.InterviewID == interviewID {
			out = append(out, score)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Dimension < out[b].Dimension })
	return out, nil
}

func (s *memoryStore) DecideScore(_ context.Context, companyID, interviewID, scoreID uuid.UUID, d ScoreDecision) (domain.Score, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Score{}, err
	}
	score, ok := s.scores[key{companyID, scoreID}]
	if !ok || score.InterviewID != interviewID {
		return domain.Score{}, ErrNotFound
	}
	score.Status, score.DecisionNote = d.Status, d.Note
	switch d.Status {
	case domain.ScoreStatusHumanApproved:
		v := score.ProposedValue
		score.FinalValue = &v
	case domain.ScoreStatusHumanRejected:
		score.FinalValue = nil
	default:
		score.FinalValue = d.FinalValue
	}
	by, at := d.DecidedBy, d.DecidedAt
	score.DecidedBy, score.DecidedAt = &by, &at
	s.scores[key{companyID, scoreID}] = score
	return score, nil
}

func (s *memoryStore) ListPolicies(_ context.Context, companyID uuid.UUID) ([]domain.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return nil, err
	}
	var out []domain.Policy
	for k, p := range s.policies {
		if k.company == companyID {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *memoryStore) SetPolicy(_ context.Context, companyID uuid.UUID, k string, enabled bool, by uuid.UUID) (domain.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.seen(companyID); err != nil {
		return domain.Policy{}, err
	}
	now := time.Now()
	p := domain.Policy{Key: k, Enabled: enabled, UpdatedBy: &by, UpdatedAt: &now}
	s.policies[policyKey{companyID, uuid.NewSHA1(uuid.Nil, []byte(k))}] = p
	return p, nil
}
