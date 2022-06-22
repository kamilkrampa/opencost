package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/kubecost/opencost/pkg/env"
	"github.com/kubecost/opencost/pkg/log"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// StorageProvider is the type of provider used for storage if not leveraging a file implementation.
type StorageProvider string

const (
	S3    StorageProvider = "S3"
	GCS   StorageProvider = "GCS"
	AZURE StorageProvider = "AZURE"
)

// StorageConfig is the configuration type used as the "parent" configuration. It contains a type, which will
// specify the bucket storage implementation, and a configuration object specific to that storage implementation.
type StorageConfig struct {
	Type   StorageProvider `yaml:"type"`
	Config interface{}     `yaml:"config"`
}

// NewBucketStorage initializes and returns new Storage implementation leveraging the storage provider
// configuration. This configuration type uses the layout provided in thanos: https://thanos.io/tip/thanos/storage.md/
func NewBucketStorage(namespaces v1.NamespaceInterface, config []byte) (Storage, error) {
	storageConfig := &StorageConfig{}
	if err := yaml.UnmarshalStrict(config, storageConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	// Because the Config property is specific to the storage implementation, we'll marshal back into yaml, and allow
	// the specific implementation to unmarshal back into a concrete configuration type.
	config, err := yaml.Marshal(storageConfig.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of storage configuration")
	}

	clusterID := getClusterIdentifier(namespaces)
	fmt.Println(clusterID)

	var storage Storage
	switch strings.ToUpper(string(storageConfig.Type)) {
	case string(S3):
		storage, err = NewS3Storage(config)
	case string(GCS):
		storage, err = NewGCSStorage(config)
	case string(AZURE):
		storage, err = NewAzureStorage(config)
	default:
		return nil, errors.Errorf("storage with type %s is not supported", storageConfig.Type)
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s client", storageConfig.Type))
	}

	return storage, nil
}

// trimLeading removes a leading / from the file name
func trimLeading(file string) string {
	if len(file) == 0 {
		return file
	}

	if file[0] == '/' {
		return file[1:]
	}
	return file
}

// trimName removes the leading directory prefix
func trimName(file string) string {
	slashIndex := strings.LastIndex(file, "/")
	if slashIndex < 0 {
		return file
	}

	name := file[slashIndex+1:]
	return name
}

func getClusterIdentifier(namespaces v1.NamespaceInterface) string {
	clusterID := env.GetClusterID()

	ns, err := namespaces.Get(context.Background(), "kube-system", metav1.GetOptions{})
	if err != nil {
		log.Errorf("Unable to get kube-system namespace: %s", err.Error())
		return ""
	}

	if clusterID != "" {
		return string(ns.UID)[:5] + "-" + clusterID
	}
	return string(ns.UID)[:10]
}
