package start

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ParsePgVersionNum_SupportedVersions_ReturnsMajorVersion(t *testing.T) {
	tests := []struct {
		name          string
		versionNumStr string
		expectedMajor int
	}{
		{name: "PG 15.0", versionNumStr: "150000", expectedMajor: 15},
		{name: "PG 15.4", versionNumStr: "150004", expectedMajor: 15},
		{name: "PG 16.0", versionNumStr: "160000", expectedMajor: 16},
		{name: "PG 16.3", versionNumStr: "160003", expectedMajor: 16},
		{name: "PG 17.2", versionNumStr: "170002", expectedMajor: 17},
		{name: "PG 18.0", versionNumStr: "180000", expectedMajor: 18},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			major, err := parsePgVersionNum(tt.versionNumStr)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedMajor, major)
			assert.GreaterOrEqual(t, major, minPgMajorVersion)
		})
	}
}

func Test_ParsePgVersionNum_UnsupportedVersions_ReturnsMajorVersionBelow15(t *testing.T) {
	tests := []struct {
		name          string
		versionNumStr string
		expectedMajor int
	}{
		{name: "PG 12.5", versionNumStr: "120005", expectedMajor: 12},
		{name: "PG 13.0", versionNumStr: "130000", expectedMajor: 13},
		{name: "PG 14.12", versionNumStr: "140012", expectedMajor: 14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			major, err := parsePgVersionNum(tt.versionNumStr)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedMajor, major)
			assert.Less(t, major, minPgMajorVersion)
		})
	}
}

func Test_ParsePgVersionNum_InvalidInput_ReturnsError(t *testing.T) {
	tests := []struct {
		name          string
		versionNumStr string
	}{
		{name: "empty string", versionNumStr: ""},
		{name: "non-numeric", versionNumStr: "abc"},
		{name: "negative number", versionNumStr: "-1"},
		{name: "zero", versionNumStr: "0"},
		{name: "float", versionNumStr: "15.4"},
		{name: "whitespace only", versionNumStr: "   "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parsePgVersionNum(tt.versionNumStr)

			require.Error(t, err)
		})
	}
}

func Test_ParsePgVersionNum_WithWhitespace_ParsesCorrectly(t *testing.T) {
	major, err := parsePgVersionNum("  150004  ")

	require.NoError(t, err)
	assert.Equal(t, 15, major)
}
