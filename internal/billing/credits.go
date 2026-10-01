package billing

import "sort"

func sortCredits(credits []Credit) {
	sort.SliceStable(credits, func(i, j int) bool {
		a, b := credits[i], credits[j]
		switch {
		case a.ExpiresAt != nil && b.ExpiresAt == nil:
			return true
		case a.ExpiresAt == nil && b.ExpiresAt != nil:
			return false
		case a.ExpiresAt != nil && !a.ExpiresAt.Equal(*b.ExpiresAt):
			return a.ExpiresAt.Before(*b.ExpiresAt)
		case !a.GrantedAt.Equal(b.GrantedAt):
			return a.GrantedAt.Before(b.GrantedAt)
		default:
			return a.ID.String() < b.ID.String()
		}
	})
}
