// SPDX-License-Identifier: Apache-2.0

// Package template builds the text/template instances used by the
// transformers that accept user provided templates. The greenmask toolkit
// function map they rely on closes over state that is not safe for concurrent
// use (a shared *rand.Rand behind randomInt/randomString/noiseInt/..., and a
// pgtype.Map behind tsModify/noiseDatePgInterval), so a single template cannot
// be executed by several snapshot workers at once. Templates are pooled
// instead: each execution gets an instance with function map state of its own.
package template

import (
	"io"
	"maps"
	texttemplate "text/template"

	"github.com/Masterminds/sprig/v3"
	greenmasktoolkit "github.com/eminano/greenmask/pkg/toolkit"
	"github.com/xataio/pgstream/pkg/transformers/internal/pool"
)

// Template is a text/template with the greenmask toolkit and sprig function
// maps available to it, safe for concurrent execution.
type Template struct {
	pool *pool.Pool[*texttemplate.Template]
}

// New parses text into a template named name. Parse errors are reported here
// rather than on first execution.
func New(name, text string) (*Template, error) {
	p, err := pool.New(func() (*texttemplate.Template, error) {
		return texttemplate.New(name).Funcs(funcMap()).Parse(text)
	})
	if err != nil {
		return nil, err
	}
	return &Template{pool: p}, nil
}

// Execute applies the template to data, writing the output to w. It can be
// called concurrently.
func (t *Template) Execute(w io.Writer, data any) error {
	tmpl, err := t.pool.Acquire()
	if err != nil {
		return err
	}
	defer t.pool.Release(tmpl)
	return tmpl.Execute(w, data)
}

// funcMap returns a function map for exclusive use by a single template
// instance. Sprig is applied last, so its functions win over the greenmask
// ones with the same name.
func funcMap() texttemplate.FuncMap {
	fm := greenmasktoolkit.FuncMap()
	maps.Copy(fm, sprig.FuncMap())
	return fm
}
