package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mustResolveProtocolSpec(t *testing.T, protocol, encodingFormat string) protocolSpec {
	t.Helper()

	spec, err := resolveProtocolSpec(protocol, encodingFormat)
	require.NoError(t, err)
	return spec
}

func TestResolveProtocolSpec(t *testing.T) {
	t.Parallel()

	cases := []struct {
		protocol       string
		encodingFormat string
		fixtureDir     string
		normalization  normalizationMode
	}{
		{
			protocol:      "canal-json",
			fixtureDir:    "canal-json",
			normalization: normalizationModeJSONKeyValue,
		},
		{
			protocol:      "open-protocol",
			fixtureDir:    "open-protocol",
			normalization: normalizationModeOpenProtocol,
		},
		{
			protocol:      "debezium",
			fixtureDir:    "debezium",
			normalization: normalizationModeJSONKeyValue,
		},
		{
			protocol:      "simple",
			fixtureDir:    "simple",
			normalization: normalizationModeJSONKeyValue,
		},
		{
			protocol:       "simple",
			encodingFormat: "avro",
			fixtureDir:     "simple-avro",
			normalization:  normalizationModeRawKeyValue,
		},
		{
			protocol:      "avro",
			fixtureDir:    "avro",
			normalization: normalizationModeRawKeyValue,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.fixtureDir, func(t *testing.T) {
			t.Parallel()

			spec, err := resolveProtocolSpec(tc.protocol, tc.encodingFormat)
			require.NoError(t, err)
			require.Equal(t, tc.protocol, spec.protocol)
			require.Equal(t, tc.fixtureDir, spec.fixtureDir)
			require.Equal(t, tc.normalization, spec.normalization)
		})
	}
}

func TestResolveProtocolSpecRejectsUnsupportedValues(t *testing.T) {
	t.Parallel()

	_, err := resolveProtocolSpec("simple", "xml")
	require.Error(t, err)

	_, err = resolveProtocolSpec("unknown", "")
	require.Error(t, err)
}

func TestConfigValidateProtocolSpecificFlags(t *testing.T) {
	t.Parallel()

	cfg := config{
		mode:         modeVerify,
		protocol:     "avro",
		sqlRoot:      "sql",
		fixtureRoot:  "fixtures",
		upstreamDSN:  "root@tcp(127.0.0.1:4000)/",
		kafkaAddrs:   "127.0.0.1:9092",
		topic:        "codec-compat",
		changefeedID: "codec-compat",
	}

	err := cfg.validate()
	require.Error(t, err)

	cfg.schemaRegistryURI = "http://127.0.0.1:8088"
	require.NoError(t, cfg.validate())

	cfg.protocol = "open-protocol"
	cfg.encodingFormat = "avro"
	err = cfg.validate()
	require.Error(t, err)
}
