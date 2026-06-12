package pg18

import (
	"os"
	"testing"

	physicaltesting "databasus-backend/internal/features/tests/physical/postgresql/shared"
)

func TestMain(m *testing.M) {
	teardown := physicaltesting.SetupNodes()
	code := m.Run()
	teardown()
	os.Exit(code)
}
