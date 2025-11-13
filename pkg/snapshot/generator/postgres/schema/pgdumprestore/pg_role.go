// SPDX-License-Identifier: Apache-2.0

package pgdumprestore

type role struct {
	name                 string
	roleDependencies     map[string]role
	schemasWithOwnership map[string]struct{}
}

type roleOption func(r *role)

func newRole(name string, opts ...roleOption) role {
	r := role{
		name:                 name,
		schemasWithOwnership: make(map[string]struct{}),
		roleDependencies:     make(map[string]role),
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

func withOwner(schema ...string) roleOption {
	return func(r *role) {
		if len(schema) == 0 {
			return
		}
		for _, s := range schema {
			if s != "" {
				r.schemasWithOwnership[s] = struct{}{}
			}
		}
	}
}

func withRoleDeps(deps ...string) roleOption {
	return func(r *role) {
		if len(deps) == 0 {
			return
		}
		for _, dep := range deps {
			if dep != "" {
				r.roleDependencies[dep] = newRole(dep)
			}
		}
	}
}

func (r *role) isOwner() bool {
	return len(r.schemasWithOwnership) > 0
}
