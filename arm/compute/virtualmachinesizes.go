package compute

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
// Code generated by Microsoft (R) AutoRest Code Generator 0.12.0.0
// Changes may cause incorrect behavior and will be lost if the code is
// regenerated.

import (
	"net/http"
	"net/url"

	"azure-sdk-for-go/Godeps/_workspace/src/github.com/Azure/go-autorest/autorest"
)

// VirtualMachineSizesClient is the the Compute Management Client.
type VirtualMachineSizesClient struct {
	ManagementClient
}

// NewVirtualMachineSizesClient creates an instance of the
// VirtualMachineSizesClient client.
func NewVirtualMachineSizesClient(subscriptionID string) VirtualMachineSizesClient {
	return NewVirtualMachineSizesClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewVirtualMachineSizesClientWithBaseURI creates an instance of the
// VirtualMachineSizesClient client.
func NewVirtualMachineSizesClientWithBaseURI(baseURI string, subscriptionID string) VirtualMachineSizesClient {
	return VirtualMachineSizesClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// List lists virtual-machine-sizes available in a location for a subscription.
//
// location is the location upon which virtual-machine-sizes is queried.
func (client VirtualMachineSizesClient) List(location string) (result VirtualMachineSizeListResult, ae error) {
	req, err := client.ListPreparer(location)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachineSizesClient", "List", "Failure preparing request")
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachineSizesClient", "List", "Failure sending request")
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachineSizesClient", "List", "Failure responding to request")
	}

	return
}

// ListPreparer prepares the List request.
func (client VirtualMachineSizesClient) ListPreparer(location string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"location":       url.QueryEscape(location),
		"subscriptionId": url.QueryEscape(client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/providers/Microsoft.Compute/locations/{location}/vmSizes"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// ListSender sends the List request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachineSizesClient) ListSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK)
}

// ListResponder handles the response to the List request. The method always
// closes the http.Response Body.
func (client VirtualMachineSizesClient) ListResponder(resp *http.Response) (result VirtualMachineSizeListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}
