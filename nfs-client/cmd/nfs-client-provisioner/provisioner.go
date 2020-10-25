/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"k8s.io/kubernetes/pkg/apis/core/v1/helper"

	"github.com/golang/glog"
	"github.com/kubernetes-sigs/sig-storage-lib-external-provisioner/controller"
	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/apimachinery/pkg/api/resource"
	"path"
)

const (
	provisionerNameKey = "PROVISIONER_NAME"
	// A PV annotation for the project quota info block, needed for quota
	// deletion.
	annProjectBlock = "Project_block"
	// A PV annotation for the project quota id, needed for quota deletion
	annProjectID = "Project_Id"
	mountPath = "/persistentvolumes"

)

var (
	kubeconfig     = flag.String("kubeconfig", "", "Absolute path to the kubeconfig file. Either this or master needs to be set if the provisioner is being run out of cluster.")
)

type nfsProvisioner struct {
	client kubernetes.Interface
	server string
	path   string
	// The quotaer to use for setting per-share/directory/project quotas
	quotaer quotaer
}

var _ controller.Provisioner = &nfsProvisioner{}

func (p *nfsProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}
	glog.V(4).Infof("nfs provisioner: VolumeOptions %v", options)

	pvcNamespace := options.PVC.Namespace
	pvcName := options.PVC.Name

	pvName := strings.Join([]string{pvcNamespace, pvcName, options.PVName}, "-")

	fullPath := filepath.Join(mountPath, pvName)
	glog.V(4).Infof("creating path %s", fullPath)
	if err := os.MkdirAll(fullPath, 0777); err != nil {
		return nil, errors.New("unable to create directory to provision new pv: " + err.Error())
	}
	//os.Chmod(fullPath, 0777)
	if err := os.Chown(fullPath, 1500, 1500); err != nil {
		return nil, errors.New("unable to chown 1500:1500 to provision new pv: " + err.Error())
	}
	path := filepath.Join(p.path, pvName)

	projectBlock, projectID, err := p.createQuota(pvName, options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)])
	if err != nil {
		os.RemoveAll(path)
		return nil, fmt.Errorf("error creating quota for volume: %v", err)
	}

	annotations := make(map[string]string)
	annotations[annProjectBlock] = projectBlock
	annotations[annProjectID] = strconv.FormatUint(uint64(projectID), 10)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: annotations,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			MountOptions:                  options.MountOptions,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{
					Server:   p.server,
					Path:     path,
					ReadOnly: false,
				},
			},
		},
	}
	return pv, nil
}

func (p *nfsProvisioner) Delete(volume *v1.PersistentVolume) error {
	path := volume.Spec.PersistentVolumeSource.NFS.Path
	pvName := filepath.Base(path)
	oldPath := filepath.Join(mountPath, pvName)
	if _, err := os.Stat(oldPath); os.IsNotExist(err) {
		glog.Warningf("path %s does not exist, deletion skipped", oldPath)
		return nil
	}
	// Get the storage class for this volume.
	storageClass, err := p.getClassForVolume(volume)
	if err != nil {
		return err
	}
	// Determine if the "archiveOnDelete" parameter exists.
	// If it exists and has a false value, delete the directory.
	// Otherwise, archive it.
	archiveOnDelete, exists := storageClass.Parameters["archiveOnDelete"]
	if exists {
		archiveBool, err := strconv.ParseBool(archiveOnDelete)
		if err != nil {
			return err
		}
		if !archiveBool {
			return os.RemoveAll(oldPath)
		}
	}

	err = p.deleteQuota(volume)
	if err != nil {
		return fmt.Errorf("deleted the volume's backing path & export but error deleting quota: %v", err)
	}

	archivePath := filepath.Join(mountPath, "archived-"+pvName)
	glog.V(4).Infof("archiving path %s to %s", oldPath, archivePath)
	return os.Rename(oldPath, archivePath)

}

// getClassForVolume returns StorageClass
func (p *nfsProvisioner) getClassForVolume(pv *v1.PersistentVolume) (*storage.StorageClass, error) {
	if p.client == nil {
		return nil, fmt.Errorf("Cannot get kube client")
	}
	className := helper.GetPersistentVolumeClass(pv)
	if className == "" {
		return nil, fmt.Errorf("Volume has no storage class")
	}
	class, err := p.client.StorageV1().StorageClasses().Get(className, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return class, nil
}

// createQuota creates a quota for the directory by adding a project to
// represent the directory and setting a quota on it
func (p *nfsProvisioner) createQuota(directory string, capacity resource.Quantity) (string, uint16, error) {
	path := path.Join(p.path, directory)

	limit := strconv.FormatInt(capacity.Value(), 10)

	block, projectID, err := p.quotaer.AddProject(path, limit)
	if err != nil {
		return "", 0, fmt.Errorf("error adding project for path %s: %v", path, err)
	}

	err = p.quotaer.SetQuota(projectID, path, limit)
	if err != nil {
		p.quotaer.RemoveProject(block, projectID)
		return "", 0, fmt.Errorf("error setting quota for path %s: %v", path, err)
	}

	return block, projectID, nil
}

func (p *nfsProvisioner) deleteQuota(volume *v1.PersistentVolume) error {
	block, projectID, err := getBlockAndID(volume, annProjectBlock, annProjectID)
	if err != nil {
		return fmt.Errorf("error getting block &/or id from annotations: %v", err)
	}

	if err := p.quotaer.RemoveProject(block, uint16(projectID)); err != nil {
		return fmt.Errorf("error removing the quota project from the projects file: %v", err)
	}

	if err := p.quotaer.UnsetQuota(); err != nil {
		return fmt.Errorf("removed quota project from the project file but error unsetting the quota: %v", err)
	}

	return nil
}

func getBlockAndID(volume *v1.PersistentVolume, annBlock, annID string) (string, uint16, error) {
	block, ok := volume.Annotations[annBlock]
	if !ok {
		return "", 0, fmt.Errorf("PV doesn't have an annotation with key %s", annBlock)
	}

	idStr, ok := volume.Annotations[annID]
	if !ok {
		return "", 0, fmt.Errorf("PV doesn't have an annotation %s", annID)
	}
	id, _ := strconv.ParseUint(idStr, 10, 16)

	return block, uint16(id), nil
}



func NewNfsClientProvisioner(clientset kubernetes.Interface, server, path string, enableXfsQuota bool)  *nfsProvisioner {
	var quotaer quotaer
	var err error
	if enableXfsQuota {
		quotaer, err = newXfsQuotaer(mountPath)
		if err != nil {
			glog.Fatalf("Error creating xfs quotaer! %v", err)
		}
	} else {
		quotaer = newDummyQuotaer()
	}

	clientNFSProvisioner := &nfsProvisioner{
		client: clientset,
		server: server,
		path:   path,
		quotaer: quotaer,
	}
	return clientNFSProvisioner
}

func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")

	server := os.Getenv("NFS_SERVER")
	if server == "" {
		glog.Fatal("NFS_SERVER not set")
	}
	path := os.Getenv("NFS_PATH")
	if path == "" {
		glog.Fatal("NFS_PATH not set")
	}
	provisionerName := os.Getenv(provisionerNameKey)
	if provisionerName == "" {
		glog.Fatalf("environment variable %s is not set! Please set it.", provisionerNameKey)
	}
	var config *rest.Config
	var err error

	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			glog.Fatalf("Failed to create config from kubeconfig with err %v", err)
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			glog.Fatalf("Failed to create in cluster config: %v", err)
		}
	}

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	clientNFSProvisioner := NewNfsClientProvisioner(clientset, server, path, true)
	// Start the provision controller which will dynamically provision efs NFS
	// PVs
	pc := controller.NewProvisionController(clientset, provisionerName, clientNFSProvisioner, serverVersion.GitVersion)
	pc.Run(wait.NeverStop)
}
