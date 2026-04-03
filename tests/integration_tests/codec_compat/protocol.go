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

import "github.com/pingcap/errors"

const (
	protocolCanalJSON   = "canal-json"
	protocolOpen        = "open-protocol"
	protocolDebezium    = "debezium"
	protocolSimple      = "simple"
	protocolAvro        = "avro"
	encodingFormatJSON  = "json"
	encodingFormatAvro  = "avro"
	fixtureDirSimpleRaw = "simple-avro"
)

type protocolSpec struct {
	protocol       string
	encodingFormat string
	fixtureDir     string
}

func resolveProtocolSpec(protocol, encodingFormat string) (protocolSpec, error) {
	switch protocol {
	case protocolCanalJSON:
		if encodingFormat != "" {
			return protocolSpec{}, errors.Errorf("encoding-format is only supported by protocol %s", protocolSimple)
		}
		return protocolSpec{
			protocol:   protocol,
			fixtureDir: protocol,
		}, nil
	case protocolOpen:
		if encodingFormat != "" {
			return protocolSpec{}, errors.Errorf("encoding-format is only supported by protocol %s", protocolSimple)
		}
		return protocolSpec{
			protocol:   protocol,
			fixtureDir: protocol,
		}, nil
	case protocolDebezium:
		if encodingFormat != "" {
			return protocolSpec{}, errors.Errorf("encoding-format is only supported by protocol %s", protocolSimple)
		}
		return protocolSpec{
			protocol:   protocol,
			fixtureDir: protocol,
		}, nil
	case protocolSimple:
		if encodingFormat == "" {
			encodingFormat = encodingFormatJSON
		}
		switch encodingFormat {
		case encodingFormatJSON:
			return protocolSpec{
				protocol:       protocol,
				encodingFormat: encodingFormat,
				fixtureDir:     protocol,
			}, nil
		case encodingFormatAvro:
			return protocolSpec{
				protocol:       protocol,
				encodingFormat: encodingFormat,
				fixtureDir:     fixtureDirSimpleRaw,
			}, nil
		default:
			return protocolSpec{}, errors.Errorf("unsupported encoding-format %s for protocol %s", encodingFormat, protocol)
		}
	case protocolAvro:
		if encodingFormat != "" {
			return protocolSpec{}, errors.Errorf("encoding-format is only supported by protocol %s", protocolSimple)
		}
		return protocolSpec{
			protocol:   protocol,
			fixtureDir: protocol,
		}, nil
	default:
		return protocolSpec{}, errors.Errorf("unsupported protocol %s", protocol)
	}
}

func (s protocolSpec) requiresSchemaRegistry() bool {
	return s.protocol == protocolAvro
}

func (s protocolSpec) usesRawEncoding() bool {
	return s.protocol == protocolAvro ||
		(s.protocol == protocolSimple && s.encodingFormat == encodingFormatAvro)
}

func (s protocolSpec) usesOpenProtocol() bool {
	return s.protocol == protocolOpen
}
