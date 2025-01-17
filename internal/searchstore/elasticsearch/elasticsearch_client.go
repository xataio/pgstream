// SPDX-License-Identifier: Apache-2.0

package elasticsearch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/xataio/pgstream/internal/searchstore"
)

type Client struct {
	client *elasticsearch.Client
}

var errInvalidSearchEnvelope = errors.New("invalid search response")

func NewClient(url string) (*Client, error) {
	es, err := newClient(url)
	if err != nil {
		return nil, fmt.Errorf("create elasticsearch client: %w", err)
	}
	return &Client{client: es}, nil
}

func (ec *Client) GetMapper() searchstore.Mapper {
	return NewMapper()
}

func (ec *Client) CloseIndex(ctx context.Context, index string) error {
	res, err := ec.client.Indices.Close(
		[]string{index},
		ec.client.Indices.Close.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[CloseIndex] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[CloseIndex] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) Count(ctx context.Context, index string) (int, error) {
	res, err := ec.client.Count(
		ec.client.Count.WithIndex(index),
		ec.client.Count.WithContext(ctx))
	if err != nil {
		return 0, fmt.Errorf("[Count] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return 0, fmt.Errorf("[Count] error response from Elasticsearch: %w", err)
	}

	count := &searchstore.CountResponse{}
	if err := json.NewDecoder(res.Body).Decode(count); err != nil {
		return 0, fmt.Errorf("[Count] error decoding Elasticsearch response: %w", err)
	}

	return count.Count, nil
}

func (ec *Client) CreateIndex(ctx context.Context, index string, body map[string]any) error {
	reader, err := searchstore.CreateReader(body)
	if err != nil {
		return err
	}
	res, err := ec.client.Indices.Create(index,
		ec.client.Indices.Create.WithContext(ctx),
		ec.client.Indices.Create.WithBody(reader),
	)
	if err != nil {
		return fmt.Errorf("[CreateIndex] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[CreateIndex] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) DeleteByQuery(ctx context.Context, req *searchstore.DeleteByQueryRequest) error {
	reader, err := searchstore.CreateReader(req.Query)
	if err != nil {
		return err
	}

	res, err := ec.client.DeleteByQuery(req.Index,
		reader,
		ec.client.DeleteByQuery.WithContext(ctx),
		ec.client.DeleteByQuery.WithSlices("auto"),
		ec.client.DeleteByQuery.WithWaitForCompletion(false),
		ec.client.DeleteByQuery.WithRefresh(req.Refresh),
	)
	if err != nil {
		return fmt.Errorf("[DeleteByQuery] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[DeleteByQuery] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) DeleteIndex(ctx context.Context, index []string) error {
	res, err := ec.client.Indices.Delete(
		index,
		ec.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[DeleteIndex] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[DeleteIndex] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) Index(ctx context.Context, req *searchstore.IndexRequest) error {
	res, err := ec.client.Index(req.Index,
		bytes.NewReader(req.Body),
		ec.client.Index.WithContext(ctx),
		ec.client.Index.WithRefresh(req.Refresh),
	)
	if err != nil {
		return fmt.Errorf("[Index] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[Index] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) IndexWithID(ctx context.Context, req *searchstore.IndexWithIDRequest) error {
	res, err := ec.client.Index(req.Index,
		bytes.NewReader(req.Body),
		ec.client.Index.WithContext(ctx),
		ec.client.Index.WithRefresh(req.Refresh),
		ec.client.Index.WithDocumentID(req.ID),
	)
	if err != nil {
		return fmt.Errorf("[IndexWithID] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[IndexWithID] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) IndexExists(ctx context.Context, index string) (bool, error) {
	res, err := ec.client.Indices.Exists([]string{index},
		ec.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, fmt.Errorf("[IndexExists] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != http.StatusNotFound {
		return false, fmt.Errorf("[IndexExists] error response from Elasticsearch: %w", err)
	}

	return res.StatusCode == http.StatusOK, nil
}

func (ec *Client) GetIndexAlias(ctx context.Context, name string) (map[string]any, error) {
	res, err := ec.client.Indices.GetAlias(
		ec.client.Indices.GetAlias.WithContext(ctx),
		ec.client.Indices.GetAlias.WithName(name),
	)
	if err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error response from Elasticsearch: %w", err)
	}

	resMap := map[string]any{}
	resData, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error reading Elasticsearch response body: %w", err)
	}

	if err := json.Unmarshal(resData, &resMap); err != nil {
		return nil, fmt.Errorf("[GetIndexAlias] error unmarshalling Elasticsearch response: %w", err)
	}
	return resMap, nil
}

func (ec *Client) GetIndexMappings(ctx context.Context, index string) (*searchstore.Mappings, error) {
	res, err := ec.client.Indices.GetMapping(
		ec.client.Indices.GetMapping.WithIndex(index),
		ec.client.Indices.GetMapping.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("[GetIndexMapping] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return nil, fmt.Errorf("[GetIndexMapping] error response from Elasticsearch: %w", err)
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
func (ec *Client) GetIndicesStats(ctx context.Context, indexPattern string) ([]searchstore.IndexStats, error) {
	res, err := ec.client.Indices.Stats(
		ec.client.Indices.Stats.WithContext(ctx),
		ec.client.Indices.Stats.WithIndex(indexPattern),
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
func (ec *Client) ListIndices(ctx context.Context, indices []string) ([]string, error) {
	res, err := ec.client.Cat.Indices(
		ec.client.Cat.Indices.WithContext(ctx),
		ec.client.Cat.Indices.WithIndex(indices...),
		ec.client.Cat.Indices.WithH("index"),
	)
	if err != nil {
		return []string{}, fmt.Errorf("[ListIndices] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return []string{}, fmt.Errorf("[ListIndices] error response from Elasticsearch: %w", err)
	}

	scanner := bufio.NewScanner(res.Body)
	scanner.Split(bufio.ScanLines)

	resp := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		resp = append(resp, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("[ListIndices] error scanning response from Elasticsearch: %w", err)
	}

	return resp, nil
}

func (ec *Client) PutIndexAlias(ctx context.Context, index []string, name string) error {
	res, err := ec.client.Indices.PutAlias(
		index,
		name,
		ec.client.Indices.PutAlias.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[PutIndexAlias] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[PutIndexAlias] error response from Elasticsearch: %w", err)
	}

	return nil
}

// PutIndexMappings add field type mapping data to a previously created ES index
// Dynamic mapping is disabled upon index creation, so it is a requirement to explicitly define mappings for each column
func (ec *Client) PutIndexMappings(ctx context.Context, index string, mapping map[string]any) error {
	reader, err := searchstore.CreateReader(mapping)
	if err != nil {
		return err
	}
	res, err := ec.client.Indices.PutMapping(
		[]string{index},
		reader,
		ec.client.Indices.PutMapping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("[PutIndexMappings] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[PutIndexMappings] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) PutIndexSettings(ctx context.Context, index string, settings map[string]any) error {
	reader, err := searchstore.CreateReader(settings)
	if err != nil {
		return err
	}
	res, err := ec.client.Indices.PutSettings(
		reader,
		ec.client.Indices.PutSettings.WithContext(ctx),
		ec.client.Indices.PutSettings.WithIndex(index))
	if err != nil {
		return fmt.Errorf("[PutIndexSettings] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[PutIndexSettings] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) RefreshIndex(ctx context.Context, index string) error {
	res, err := ec.client.Indices.Refresh(
		ec.client.Indices.Refresh.WithIndex(index),
		ec.client.Indices.Refresh.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("[RefreshIndex] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if err := ec.isErrResponse(res); err != nil {
		return fmt.Errorf("[RefreshIndex] error response from Elasticsearch: %w", err)
	}

	return nil
}

func (ec *Client) Perform(req *http.Request) (*http.Response, error) {
	return ec.client.Transport.Perform(req)
}

func (ec *Client) Search(ctx context.Context, req *searchstore.SearchRequest) (*searchstore.SearchResponse, error) {
	res, err := ec.client.Search(ec.parseSearchRequest(ctx, req)...)
	if err != nil {
		return nil, fmt.Errorf("[Search] error from Elasticsearch: %w", err)
	}
	defer res.Body.Close()
	if err := ec.isErrResponse(res); err != nil {
		return nil, fmt.Errorf("[Search] error response from Elasticsearch: %w", err)
	}

	var response searchstore.SearchResponse
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("[Search] decoding response body: %w: %w", errInvalidSearchEnvelope, err)
	}

	return &response, nil
}

// SendBulkRequest can perform multiple indexing or delete operations in a single call
func (ec *Client) SendBulkRequest(ctx context.Context, items []searchstore.BulkItem) ([]searchstore.BulkItem, error) {
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

	resp, err := ec.Perform(req)
	if err != nil {
		return nil, fmt.Errorf("perform: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		return nil, fmt.Errorf("[SendBulkRequest] error response from Elasticsearch: %w", searchstore.ExtractResponseError(resp.Body, resp.StatusCode))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return searchstore.VerifyResponse(bodyBytes, items)
}

func (ec *Client) parseSearchRequest(ctx context.Context, req *searchstore.SearchRequest) []func(*esapi.SearchRequest) {
	opts := []func(*esapi.SearchRequest){
		ec.client.Search.WithContext(ctx),
	}
	if req.Index != nil {
		opts = append(opts, ec.client.Search.WithIndex(*req.Index))
	}
	if req.ReturnVersion != nil {
		opts = append(opts, ec.client.Search.WithVersion(*req.ReturnVersion))
	}
	if req.Size != nil {
		opts = append(opts, ec.client.Search.WithSize(*req.Size))
	}
	if req.From != nil {
		opts = append(opts, ec.client.Search.WithFrom(*req.From))
	}
	if req.Sort != nil {
		opts = append(opts, ec.client.Search.WithSort(*req.Sort))
	}
	if req.Query != nil {
		opts = append(opts, ec.client.Search.WithBody(req.Query))
	}
	if req.SourceIncludes != nil {
		opts = append(opts, ec.client.Search.WithSourceIncludes(*req.SourceIncludes))
	}

	return opts
}

func (ec *Client) isErrResponse(res *esapi.Response) error {
	return searchstore.IsErrResponse(newAPIResponse(res))
}

func newClient(address string) (*elasticsearch.Client, error) {
	if address == "" {
		return nil, errors.New("no address provided")
	}

	cfg := elasticsearch.Config{
		Addresses: []string{
			address,
		},
		Transport: http.DefaultTransport,
	}

	return elasticsearch.NewClient(cfg)
}

type apiResponse struct {
	*esapi.Response
}

func newAPIResponse(res *esapi.Response) *apiResponse {
	return &apiResponse{Response: res}
}

func (r *apiResponse) GetBody() io.ReadCloser {
	return r.Body
}

func (r *apiResponse) GetStatusCode() int {
	return r.StatusCode
}
