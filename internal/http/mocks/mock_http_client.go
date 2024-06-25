// SPDX-License-Identifier: Apache-2.0

package mocks

import "net/http"

type Client struct {
	DoFn func(*http.Request) (*http.Response, error)
}

func (m *Client) Do(req *http.Request) (*http.Response, error) {
	return m.DoFn(req)
}
