package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetClickHouseTestImage(t *testing.T) {
	t.Run("defaults to DockerHub latest", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_IMAGE", "")
		t.Setenv("CLICKHOUSE_VERSION", "")

		require.Equal(t, "clickhouse/clickhouse-server:latest", GetClickHouseTestImage())
	})

	t.Run("uses version as DockerHub tag", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_IMAGE", "")
		t.Setenv("CLICKHOUSE_VERSION", "25.8-lts-decimal512-2026-03-27")

		require.Equal(t, "clickhouse/clickhouse-server:25.8-lts-decimal512-2026-03-27", GetClickHouseTestImage())
	})

	t.Run("prefers full image override", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_IMAGE", "registry.example.com/clickhouse/clickhouse-server:custom")
		t.Setenv("CLICKHOUSE_VERSION", "latest")

		require.Equal(t, "registry.example.com/clickhouse/clickhouse-server:custom", GetClickHouseTestImage())
	})
}
