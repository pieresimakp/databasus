package storages

import (
	"github.com/google/uuid"

	local_storage "databasus-backend/internal/features/storages/models/local"
)

func CreateTestStorage(workspaceID uuid.UUID) *Storage {
	storage := &Storage{
		WorkspaceID:  workspaceID,
		Type:         StorageTypeLocal,
		Name:         "Test Storage " + uuid.New().String(),
		LocalStorage: &local_storage.LocalStorage{},
	}

	storage, err := storageRepository.Save(storage)
	if err != nil {
		panic(err)
	}

	return storage
}

func RemoveTestStorage(id uuid.UUID) {
	storage, err := storageRepository.FindByID(id)
	if err != nil {
		panic(err)
	}

	err = storageRepository.Delete(storage)
	if err != nil {
		panic(err)
	}
}
