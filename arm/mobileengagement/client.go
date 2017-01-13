// Package mobileengagement implements the Azure ARM Mobileengagement service
// API version 2014-12-01.
//
// Microsoft Azure Mobile Engagement REST APIs.
package mobileengagement

// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by Microsoft (R) AutoRest Code Generator 0.17.0.0
// Changes may cause incorrect behavior and will be lost if the code is
// regenerated.

import (
	"github.com/Azure/go-autorest/autorest"
)

const (
	// APIVersion is the version of the Mobileengagement
	APIVersion = "2014-12-01"

	// DefaultBaseURI is the default URI used for the service Mobileengagement
	DefaultBaseURI = "https://management.azure.com"
)

// ManagementClient is the base client for Mobileengagement.
type ManagementClient struct {
	autorest.Client
	BaseURI           string
	APIVersion        string
	SubscriptionID    string
	ResourceGroupName string
	AppCollection     string
	AppName           string
}

// New creates an instance of the ManagementClient client.
func New(subscriptionID string, resourceGroupName string, appCollection string, appName string) ManagementClient {
	return NewWithBaseURI(DefaultBaseURI, subscriptionID, resourceGroupName, appCollection, appName)
}

// NewWithBaseURI creates an instance of the ManagementClient client.
func NewWithBaseURI(baseURI string, subscriptionID string, resourceGroupName string, appCollection string, appName string) ManagementClient {
	return ManagementClient{
		Client:            autorest.NewClientWithUserAgent(UserAgent()),
		BaseURI:           baseURI,
		APIVersion:        APIVersion,
		SubscriptionID:    subscriptionID,
		ResourceGroupName: resourceGroupName,
		AppCollection:     appCollection,
		AppName:           appName,
	}
}
