// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/pingcap/errors"
)

type ChangefeedClient struct {
	baseURL      *url.URL
	changefeedID string
	keyspace     string
	httpClient   *http.Client
}

type changefeedInfo struct {
	CheckpointTs uint64 `json:"checkpoint_ts"`
}

func NewChangefeedClient(rawURL, changefeedID, keyspace string) *ChangefeedClient {
	parsed, _ := url.Parse(rawURL)
	return &ChangefeedClient{
		baseURL:      parsed,
		changefeedID: changefeedID,
		keyspace:     keyspace,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *ChangefeedClient) WaitCheckpoint(
	ctx context.Context,
	target uint64,
	pollInterval time.Duration,
) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		checkpoint, err := c.CheckpointTS(ctx)
		if err != nil {
			return err
		}
		if checkpoint >= target {
			return nil
		}

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *ChangefeedClient) CheckpointTS(ctx context.Context) (uint64, error) {
	if c.baseURL == nil {
		return 0, errors.New("cdc api url is invalid")
	}

	u := *c.baseURL
	u.Path = path.Join(u.Path, "/api/v2/changefeeds", c.changefeedID)
	query := u.Query()
	query.Set("keyspace", c.keyspace)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, errors.Trace(err)
	}
	req.SetBasicAuth("ticdc", "ticdc_secret")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, errors.Errorf("query changefeed failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var info changefeedInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return 0, errors.Trace(err)
	}

	return info.CheckpointTs, nil
}
