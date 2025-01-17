// SPDX-License-Identifier: Apache-2.0

package opensearch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/xataio/pgstream/internal/searchstore"
)

type Client struct {
	client *opensearch.Client
}

var errInvalidSearchEnvelope = errors.New("invalid search response")

func NewClient(url string) (*Client, error) {
	os, err := newClient(url)
	if err != nil {
		return nil, fmt.Errorf("create opensearch client: %w", err)
	}
	return &Client{client: os}, nil
}

func (c *Client) GetMapper() searchstore.Mapper {
	return NewMapper()
}

func (c *Client) CloseIndex(ctx context.Context, index string) error {
	res, err := c.client.Indices.Close(
		[]string{index},
		c.client.Indices.Close.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[CloseIndex] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[CloseIndex] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) Count(ctx context.Context, index string) (int, error) {
	res, err := c.client.Count(
		c.client.Count.WithIndex(index),
		c.client.Count.WithContext(ctx))
	if err != nil {
		return 0, fmt.Errorf("[Count] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return 0, fmt.Errorf("[Count] error response from OpenSearch: %w", err)
	}

	count := &searchstore.CountResponse{}
	if err := json.NewDecoder(res.Body).Decode(count); err != nil {
		return 0, fmt.Errorf("[Count] error decoding OpenSearch response: %w", err)
	}

	return count.Count, nil
}

func (c *Client) CreateIndex(ctx context.Context, index string, body map[string]any) error {
	reader, err := searchstore.CreateReader(body)
	if err != nil {
		return err
	}
	res, err := c.client.Indices.Create(index,
		c.client.Indices.Create.WithContext(ctx),
		c.client.Indices.Create.WithBody(reader),
	)
	if err != nil {
		return fmt.Errorf("[CreateIndex] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[CreateIndex] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) DeleteByQuery(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
	reader, err := searchstore.CreateReader(req.Query)
	if err != nil {
		return err
	}

	res, err := c.client.DeleteByQuery(req.Index,
		reader,
		c.client.DeleteByQuery.WithContext(ctx),
		c.client.DeleteByQuery.WithSlices("auto"),
		c.client.DeleteByQuery.WithWaitForCompletion(false),
		c.client.DeleteByQuery.WithRefresh(req.Refresh),
	)
	if err != nil {
		return fmt.Errorf("[DeleteByQuery] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[DeleteByQuery] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) DeleteIndex(ctx context.Context, index []string) error {
	res, err := c.client.Indices.Delete(
		index,
		c.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[DeleteIndex] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[DeleteIndex] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) Index(ctx context.Context, req *searchstore.IndexRequest) error {
	res, err := c.client.Index(req.Index,
		bytes.NewReader(req.Body),
		c.client.Index.WithContext(ctx),
		c.client.Index.WithRefresh(req.Refresh),
	)
	if err != nil {
		return fmt.Errorf("[Index] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[Index] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) IndexWithID(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
	res, err := c.client.Index(req.Index,
		bytes.NewReader(req.Body),
		c.client.Index.WithContext(ctx),
		c.client.Index.WithRefresh(req.Refresh),
		c.client.Index.WithDocumentID(req.ID),
	)
	if err != nil {
		return fmt.Errorf("[IndexWithID] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[IndexWithID] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) IndexExists(ctx context.Context, index string) (bool, error) {
	res, err := c.client.Indices.Exists([]string{index},
		c.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, fmt.Errorf("[IndexExists] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != http.StatusNotFound {
		return false, fmt.Errorf("[IndexExists] error response from OpenSearch: %w", err)
	}

	return res.StatusCode == http.StatusOK, nil
}

func (c *Client) GetIndexAlias(ctx context.Context, name string) (map[string]any, error) {
	res, err := c.client.Indices.GetAlias(
		c.client.Indices.GetAlias.WithContext(ctx),
		c.client.Indices.GetAlias.WithName(name),
	)
	if err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error response from OpenSearch: %w", err)
	}

	resMap := map[string]any{}
	resData, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error reading OpenSearch response body: %w", err)
	}

	if err := json.Unmarshal(resData, &resMap); err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error unmarshalling OpenSearch response: %w", err)
	}
	return resMap, nil
}

func (c *Client) GetIndexMappings(ctx context.Context, index string) (*searchstore.Mappings, error) {
	res, err := c.client.Indices.GetMapping(
		c.client.Indices.GetMapping.WithIndex(index),
		c.client.Indices.GetMapping.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("[GetIndexMapping] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return nil, fmt.Errorf("[GetIndexMapping] error response from OpenSearch: %w", err)
	}

	var indexMappings searchstore.MappingResponse
	if err = json.NewDecoder(res.Body).Decode(&indexMappings); err != nil {
		return nil, err
	}

	mappings := indexMappings[index]

	return &mappings.Mappings, nil
}

// GetIndicesStats uses the index stats API to fetch statistics about indices. indexPattern is a
// wildcard pattern used to select the indices we care about.
func (c *Client) GetIndicesStats(ctx context.Context, indexPattern string) ([]searchstore.IndexStats, error) {
	res, err := c.client.Indices.Stats(
		c.client.Indices.Stats.WithContext(ctx),
		c.client.Indices.Stats.WithIndex(indexPattern),
	)
	if err != nil {
		return nil, fmt.Errorf("[GetIndicesStats] querying OpenSearch Cat API: %w", err)
	}
	defer res.Body.Close()

	var response searchstore.IndexStatsResponse
	if err = json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("[GetIndicesStats] decoding response body: %w", err)
	}

	usage := make([]searchstore.IndexStats, 0, len(response.Indices))
	for index, r := range response.Indices {
		usage = append(usage, searchstore.IndexStats{
			Index:            index,
			TotalSizeBytes:   uint64(r.Total.Store.SizeInBytes),
			PrimarySizeBytes: uint64(r.Primaries.Store.SizeInBytes),
		})
	}

	return usage, nil
}

// ListIndices returns the list of indices that match the index name pattern on
// input from the OS cluster
func (c *Client) ListIndices(ctx context.Context, indices []string) ([]string, error) {
	res, err := c.client.Cat.Indices(
		c.client.Cat.Indices.WithContext(ctx),
		c.client.Cat.Indices.WithIndex(indices...),
		c.client.Cat.Indices.WithH("index"),
	)
	if err != nil {
		return []string{}, fmt.Errorf("[ListIndices] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return []string{}, fmt.Errorf("[ListIndices] error response from OpenSearch: %w", err)
	}

	scanner := bufio.NewScanner(res.Body)
	scanner.Split(bufio.ScanLines)

	resp := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		resp = append(resp, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("[ListIndices] error scanning response from OpenSearch: %w", err)
	}

	return resp, nil
}

func (c *Client) PutIndexAlias(ctx context.Context, index []string, name string) error {
	res, err := c.client.Indices.PutAlias(
		index,
		name,
		c.client.Indices.PutAlias.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[PutIndexAlias] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[PutIndexAlias] error response from OpenSearch: %w", err)
	}

	return nil
}

// PutIndexMappings add field type mapping data to a previously created OpenSearch index
// Dynamic mapping is disabled upon index creation, so it is a requirement to explicitly define mappings for each column
func (c *Client) PutIndexMappings(ctx context.Context, index string, mapping map[string]any) error {
	reader, err := searchstore.CreateReader(mapping)
	if err != nil {
		return err
	}
	res, err := c.client.Indices.PutMapping(
		reader,
		c.client.Indices.PutMapping.WithIndex(index),
		c.client.Indices.PutMapping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("[PutIndexMappings] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[PutIndexMappings] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) PutIndexSettings(ctx context.Context, index string, settings map[string]any) error {
	reader, err := searchstore.CreateReader(settings)
	if err != nil {
		return err
	}
	res, err := c.client.Indices.PutSettings(
		reader,
		c.client.Indices.PutSettings.WithContext(ctx),
		c.client.Indices.PutSettings.WithIndex(index))
	if err != nil {
		return fmt.Errorf("[PutIndexSettings] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[PutIndexSettings] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) RefreshIndex(ctx context.Context, index string) error {
	res, err := c.client.Indices.Refresh(
		c.client.Indices.Refresh.WithIndex(index),
		c.client.Indices.Refresh.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[RefreshIndex] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()

	if err := c.isErrResponse(res); err != nil {
		return fmt.Errorf("[RefreshIndex] error response from OpenSearch: %w", err)
	}

	return nil
}

func (c *Client) Perform(req *http.Request) (*http.Response, error) {
	return c.client.Transport.Perform(req)
}

func (c *Client) Search(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
	res, err := c.client.Search(c.parseSearchRequest(ctx, req)...)
	if err != nil {
		return nil, fmt.Errorf("[Search] error from OpenSearch: %w", err)
	}
	defer res.Body.Close()
	if err := c.isErrResponse(res); err != nil {
		return nil, fmt.Errorf("[Search] error response from OpenSearch: %w", err)
	}

	var response searchstore.SearchResponse
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("[Search] decoding response body: %w: %w", errInvalidSearchEnvelope, err)
	}

	return &response, nil
}

// SendBulkRequest can perform multiple indexing or delete operations in a single call
func (c *Client) SendBulkRequest(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error) {
	buffer := new(bytes.Buffer)

	if err := searchstore.EncodeBulkItems(buffer, items); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", "/_bulk", buffer)
	if err != nil {
		return nil, fmt.Errorf("new http request: %w", err)
	}
	req.Header.Add("Content-Type", "application/x-ndjson")
	req = req.WithContext(ctx)

	resp, err := c.Perform(req)
	if err != nil {
		return nil, fmt.Errorf("perform: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		return nil, fmt.Errorf("[SendBulkRequest] error response from OpenSearch: %w", searchstore.ExtractResponseError(resp.Body, resp.StatusCode))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return searchstore.VerifyResponse(bodyBytes, items)
}

func (c *Client) parseSearchRequest(ctx context.Context, req *searchstore.SearchRequest) []func(*opensearchapi.SearchRequest) {
	opts := []func(*opensearchapi.SearchRequest){
		c.client.Search.WithContext(ctx),
	}
	if req.Index != nil {
		opts = append(opts, c.client.Search.WithIndex(*req.Index))
	}
	if req.ReturnVersion != nil {
		opts = append(opts, c.client.Search.WithVersion(*req.ReturnVersion))
	}
	if req.Size != nil {
		opts = append(opts, c.client.Search.WithSize(*req.Size))
	}
	if req.From != nil {
		opts = append(opts, c.client.Search.WithFrom(*req.From))
	}
	if req.Sort != nil {
		opts = append(opts, c.client.Search.WithSort(*req.Sort))
	}
	if req.Query != nil {
		opts = append(opts, c.client.Search.WithBody(req.Query))
	}
	if req.SourceIncludes != nil {
		opts = append(opts, c.client.Search.WithSourceIncludes(*req.SourceIncludes))
	}

	return opts
}

func (c *Client) isErrResponse(res *opensearchapi.Response) error {
	return searchstore.IsErrResponse(newAPIResponse(res))
}

func newClient(address string) (*opensearch.Client, error) {
	if address == "" {
		return nil, errors.New("no address provided")
	}

	cfg := opensearch.Config{
		Addresses: []string{
			address,
		},
		Transport: http.DefaultTransport,
	}

	return opensearch.NewClient(cfg)
}

type apiResponse struct {
	*opensearchapi.Response
}

func newAPIResponse(res *opensearchapi.Response) *apiResponse {
	return &apiResponse{Response: res}
}

func (r *apiResponse) GetBody() io.ReadCloser {
	return r.Body
}

func (r *apiResponse) GetStatusCode() int {
	return r.StatusCode
}
