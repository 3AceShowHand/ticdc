// Copyright 2025 PingCAP, Inc.
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

package common

import (
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCodecConfigApplyOutputRowKeyFromSinkConfig(t *testing.T) {
	t.Parallel()

	sinkURI, err := url.Parse("file:///tmp/test?protocol=canal-json&enable-tidb-extension=true")
	require.NoError(t, err)

	sinkConfig := &config.SinkConfig{
		KafkaConfig: &config.KafkaConfig{
			CodecConfig: &config.CodecConfig{
				OutputRowKey: util.AddressOf(true),
			},
		},
	}

	codecCfg := NewConfig(config.ProtocolCanalJSON)
	err = codecCfg.Apply(sinkURI, sinkConfig)
	require.NoError(t, err)
	require.True(t, codecCfg.OutputRowKey)
}

func TestCodecConfigApplyOutputRowKeySinkURIOverride(t *testing.T) {
	t.Parallel()

	sinkURI, err := url.Parse("file:///tmp/test?protocol=canal-json&enable-tidb-extension=true&output-row-key=true")
	require.NoError(t, err)

	sinkConfig := &config.SinkConfig{
		KafkaConfig: &config.KafkaConfig{
			CodecConfig: &config.CodecConfig{
				OutputRowKey: util.AddressOf(false),
			},
		},
	}

	codecCfg := NewConfig(config.ProtocolCanalJSON)
	err = codecCfg.Apply(sinkURI, sinkConfig)
	require.NoError(t, err)
	require.True(t, codecCfg.OutputRowKey)
}

