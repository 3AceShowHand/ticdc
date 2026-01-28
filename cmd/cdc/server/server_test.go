// Copyright 2024 PingCAP, Inc.
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

package server

import (
	"testing"

	tiflowServer "github.com/pingcap/tiflow/pkg/cmd/server"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestRunTiFlowServerPopulatesSecurityConfig(t *testing.T) {
	// This test verifies that TLS credentials from CLI flags are properly
	// transferred to tiflow options and ServerConfig.Security.
	// See https://github.com/pingcap/ticdc/issues/3718

	o := newOptions()
	o.pdEndpoints = []string{"http://pd-1:2379", "http://pd-2:2379"}
	o.serverConfigFilePath = "/path/to/config.toml"
	o.caPath = "/path/to/ca.crt"
	o.certPath = "/path/to/server.crt"
	o.keyPath = "/path/to/server.key"
	o.allowedCertCN = "cn1,cn2"

	require.Empty(t, o.serverConfig.Security.CAPath)
	require.Empty(t, o.serverConfig.Security.CertPath)
	require.Empty(t, o.serverConfig.Security.KeyPath)

	oldRun := tiflowServerRun
	var gotOptions *tiflowServer.Options
	tiflowServerRun = func(opt *tiflowServer.Options, _ *cobra.Command) error {
		gotOptions = opt
		return nil
	}
	t.Cleanup(func() { tiflowServerRun = oldRun })

	err := runTiFlowServer(o, &cobra.Command{})
	require.NoError(t, err)
	require.NotNil(t, gotOptions)
	require.NotNil(t, gotOptions.ServerConfig)

	require.Equal(t, "/path/to/config.toml", gotOptions.ServerConfigFilePath)
	require.Equal(t, "http://pd-1:2379,http://pd-2:2379", gotOptions.ServerPdAddr)
	require.Equal(t, "/path/to/ca.crt", gotOptions.CaPath)
	require.Equal(t, "/path/to/server.crt", gotOptions.CertPath)
	require.Equal(t, "/path/to/server.key", gotOptions.KeyPath)
	require.Equal(t, "cn1,cn2", gotOptions.AllowedCertCN)

	require.Equal(t, "/path/to/ca.crt", gotOptions.ServerConfig.Security.CAPath)
	require.Equal(t, "/path/to/server.crt", gotOptions.ServerConfig.Security.CertPath)
	require.Equal(t, "/path/to/server.key", gotOptions.ServerConfig.Security.KeyPath)
	require.Equal(t, []string{"cn1", "cn2"}, gotOptions.ServerConfig.Security.CertAllowedCN)
}

func TestGetCredential(t *testing.T) {
	testCases := []struct {
		name           string
		caPath         string
		certPath       string
		keyPath        string
		allowedCertCN  string
		expectedCAPath string
		expectedCert   string
		expectedKey    string
		expectedCertCN []string
	}{
		{
			name:           "all fields populated",
			caPath:         "/ca/path",
			certPath:       "/cert/path",
			keyPath:        "/key/path",
			allowedCertCN:  "test-cn",
			expectedCAPath: "/ca/path",
			expectedCert:   "/cert/path",
			expectedKey:    "/key/path",
			expectedCertCN: []string{"test-cn"},
		},
		{
			name:           "multiple CNs",
			allowedCertCN:  "cn1,cn2,cn3",
			expectedCertCN: []string{"cn1", "cn2", "cn3"},
		},
		{
			name:           "empty CN",
			allowedCertCN:  "",
			expectedCertCN: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := newOptions()
			o.caPath = tc.caPath
			o.certPath = tc.certPath
			o.keyPath = tc.keyPath
			o.allowedCertCN = tc.allowedCertCN

			cred := o.getCredential()

			require.Equal(t, tc.expectedCAPath, cred.CAPath)
			require.Equal(t, tc.expectedCert, cred.CertPath)
			require.Equal(t, tc.expectedKey, cred.KeyPath)
			require.Equal(t, tc.expectedCertCN, cred.CertAllowedCN)
		})
	}
}

func TestNewOptionsDefaultSecurity(t *testing.T) {
	o := newOptions()

	// serverConfig should be initialized with default values
	require.NotNil(t, o.serverConfig)
	require.NotNil(t, o.serverConfig.Security)

	// Security should have empty paths by default
	require.Empty(t, o.serverConfig.Security.CAPath)
	require.Empty(t, o.serverConfig.Security.CertPath)
	require.Empty(t, o.serverConfig.Security.KeyPath)
}
