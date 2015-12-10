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

// VirtualMachinesClient is the the Compute Management Client.
type VirtualMachinesClient struct {
	ManagementClient
}

// NewVirtualMachinesClient creates an instance of the VirtualMachinesClient
// client.
func NewVirtualMachinesClient(subscriptionID string) VirtualMachinesClient {
	return NewVirtualMachinesClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewVirtualMachinesClientWithBaseURI creates an instance of the
// VirtualMachinesClient client.
func NewVirtualMachinesClientWithBaseURI(baseURI string, subscriptionID string) VirtualMachinesClient {
	return VirtualMachinesClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// Capture captures the VM by copying VirtualHardDisks of the VM and outputs a
// template that can be used to create similar VMs.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine. parameters is parameters supplied to the Capture
// Virtual Machine operation.
func (client VirtualMachinesClient) Capture(resourceGroupName string, vmName string, parameters VirtualMachineCaptureParameters) (result VirtualMachineCaptureResult, ae error) {
	req, err := client.CapturePreparer(resourceGroupName, vmName, parameters)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Capture", "Failure preparing request")
	}

	resp, err := client.CaptureSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Capture", "Failure sending request")
	}

	result, err = client.CaptureResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Capture", "Failure responding to request")
	}

	return
}

// CapturePreparer prepares the Capture request.
func (client VirtualMachinesClient) CapturePreparer(resourceGroupName string, vmName string, parameters VirtualMachineCaptureParameters) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/capture"),
		autorest.WithJSON(parameters),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// CaptureSender sends the Capture request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) CaptureSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK, http.StatusAccepted)
}

// CaptureResponder handles the response to the Capture request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) CaptureResponder(resp *http.Response) (result VirtualMachineCaptureResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// CreateOrUpdate the operation to create or update a virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine. parameters is parameters supplied to the Create
// Virtual Machine operation.
func (client VirtualMachinesClient) CreateOrUpdate(resourceGroupName string, vmName string, parameters VirtualMachine) (result VirtualMachine, ae error) {
	req, err := client.CreateOrUpdatePreparer(resourceGroupName, vmName, parameters)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "CreateOrUpdate", "Failure preparing request")
	}

	resp, err := client.CreateOrUpdateSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "CreateOrUpdate", "Failure sending request")
	}

	result, err = client.CreateOrUpdateResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "CreateOrUpdate", "Failure responding to request")
	}

	return
}

// CreateOrUpdatePreparer prepares the CreateOrUpdate request.
func (client VirtualMachinesClient) CreateOrUpdatePreparer(resourceGroupName string, vmName string, parameters VirtualMachine) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPut(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}"),
		autorest.WithJSON(parameters),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// CreateOrUpdateSender sends the CreateOrUpdate request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) CreateOrUpdateSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusCreated, http.StatusOK)
}

// CreateOrUpdateResponder handles the response to the CreateOrUpdate request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) CreateOrUpdateResponder(resp *http.Response) (result VirtualMachine, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusCreated, http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// Deallocate shuts down the Virtual Machine and releases the compute
// resources. You are not billed for the compute resources that this Virtual
// Machine uses.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) Deallocate(resourceGroupName string, vmName string) (result autorest.Response, ae error) {
	req, err := client.DeallocatePreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Deallocate", "Failure preparing request")
	}

	resp, err := client.DeallocateSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Deallocate", "Failure sending request")
	}

	result, err = client.DeallocateResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Deallocate", "Failure responding to request")
	}

	return
}

// DeallocatePreparer prepares the Deallocate request.
func (client VirtualMachinesClient) DeallocatePreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/deallocate"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// DeallocateSender sends the Deallocate request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) DeallocateSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK, http.StatusAccepted)
}

// DeallocateResponder handles the response to the Deallocate request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) DeallocateResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Delete the operation to delete a virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) Delete(resourceGroupName string, vmName string) (result autorest.Response, ae error) {
	req, err := client.DeletePreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Delete", "Failure preparing request")
	}

	resp, err := client.DeleteSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Delete", "Failure sending request")
	}

	result, err = client.DeleteResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Delete", "Failure responding to request")
	}

	return
}

// DeletePreparer prepares the Delete request.
func (client VirtualMachinesClient) DeletePreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsDelete(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// DeleteSender sends the Delete request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) DeleteSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK, http.StatusAccepted, http.StatusNoContent)
}

// DeleteResponder handles the response to the Delete request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) DeleteResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted, http.StatusNoContent),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Generalize sets the state of the VM as Generalized.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) Generalize(resourceGroupName string, vmName string) (result autorest.Response, ae error) {
	req, err := client.GeneralizePreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Generalize", "Failure preparing request")
	}

	resp, err := client.GeneralizeSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Generalize", "Failure sending request")
	}

	result, err = client.GeneralizeResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Generalize", "Failure responding to request")
	}

	return
}

// GeneralizePreparer prepares the Generalize request.
func (client VirtualMachinesClient) GeneralizePreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/generalize"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// GeneralizeSender sends the Generalize request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) GeneralizeSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK)
}

// GeneralizeResponder handles the response to the Generalize request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) GeneralizeResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Get the operation to get a virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine. expand is the expand expression to apply on the
// operation.
func (client VirtualMachinesClient) Get(resourceGroupName string, vmName string, expand string) (result VirtualMachine, ae error) {
	req, err := client.GetPreparer(resourceGroupName, vmName, expand)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Get", "Failure preparing request")
	}

	resp, err := client.GetSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Get", "Failure sending request")
	}

	result, err = client.GetResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Get", "Failure responding to request")
	}

	return
}

// GetPreparer prepares the Get request.
func (client VirtualMachinesClient) GetPreparer(resourceGroupName string, vmName string, expand string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}
	if len(expand) > 0 {
		queryParameters["$expand"] = expand
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// GetSender sends the Get request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) GetSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK)
}

// GetResponder handles the response to the Get request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) GetResponder(resp *http.Response) (result VirtualMachine, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// List the operation to list virtual machines under a resource group.
//
// resourceGroupName is the name of the resource group.
func (client VirtualMachinesClient) List(resourceGroupName string) (result VirtualMachineListResult, ae error) {
	req, err := client.ListPreparer(resourceGroupName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "List", "Failure preparing request")
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "List", "Failure sending request")
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "List", "Failure responding to request")
	}

	return
}

// ListPreparer prepares the List request.
func (client VirtualMachinesClient) ListPreparer(resourceGroupName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// ListSender sends the List request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) ListSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK)
}

// ListResponder handles the response to the List request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) ListResponder(resp *http.Response) (result VirtualMachineListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListNextResults retrieves the next set of results, if any.
func (client VirtualMachinesClient) ListNextResults(lastResults VirtualMachineListResult) (result VirtualMachineListResult, ae error) {
	req, err := lastResults.VirtualMachineListResultPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "List", "Failure preparing next results request request")
	}
	if req == nil {
		return
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "List", "Failure sending next results request request")
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "List", "Failure responding to next results request request")
	}

	return
}

// ListAll gets the list of Virtual Machines in the subscription. Use nextLink
// property in the response to get the next page of Virtual Machines. Do this
// till nextLink is not null to fetch all the Virtual Machines.
func (client VirtualMachinesClient) ListAll() (result VirtualMachineListResult, ae error) {
	req, err := client.ListAllPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAll", "Failure preparing request")
	}

	resp, err := client.ListAllSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAll", "Failure sending request")
	}

	result, err = client.ListAllResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAll", "Failure responding to request")
	}

	return
}

// ListAllPreparer prepares the ListAll request.
func (client VirtualMachinesClient) ListAllPreparer() (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"subscriptionId": url.QueryEscape(client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/providers/Microsoft.Compute/virtualMachines"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// ListAllSender sends the ListAll request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) ListAllSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK)
}

// ListAllResponder handles the response to the ListAll request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) ListAllResponder(resp *http.Response) (result VirtualMachineListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListAllNextResults retrieves the next set of results, if any.
func (client VirtualMachinesClient) ListAllNextResults(lastResults VirtualMachineListResult) (result VirtualMachineListResult, ae error) {
	req, err := lastResults.VirtualMachineListResultPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAll", "Failure preparing next results request request")
	}
	if req == nil {
		return
	}

	resp, err := client.ListAllSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAll", "Failure sending next results request request")
	}

	result, err = client.ListAllResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAll", "Failure responding to next results request request")
	}

	return
}

// ListAvailableSizes lists virtual-machine-sizes available to be used for a
// virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) ListAvailableSizes(resourceGroupName string, vmName string) (result VirtualMachineSizeListResult, ae error) {
	req, err := client.ListAvailableSizesPreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAvailableSizes", "Failure preparing request")
	}

	resp, err := client.ListAvailableSizesSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAvailableSizes", "Failure sending request")
	}

	result, err = client.ListAvailableSizesResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "ListAvailableSizes", "Failure responding to request")
	}

	return
}

// ListAvailableSizesPreparer prepares the ListAvailableSizes request.
func (client VirtualMachinesClient) ListAvailableSizesPreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/vmSizes"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// ListAvailableSizesSender sends the ListAvailableSizes request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) ListAvailableSizesSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK)
}

// ListAvailableSizesResponder handles the response to the ListAvailableSizes request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) ListAvailableSizesResponder(resp *http.Response) (result VirtualMachineSizeListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// PowerOff the operation to power off (stop) a virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) PowerOff(resourceGroupName string, vmName string) (result autorest.Response, ae error) {
	req, err := client.PowerOffPreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "PowerOff", "Failure preparing request")
	}

	resp, err := client.PowerOffSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "PowerOff", "Failure sending request")
	}

	result, err = client.PowerOffResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "PowerOff", "Failure responding to request")
	}

	return
}

// PowerOffPreparer prepares the PowerOff request.
func (client VirtualMachinesClient) PowerOffPreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/powerOff"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// PowerOffSender sends the PowerOff request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) PowerOffSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK, http.StatusAccepted)
}

// PowerOffResponder handles the response to the PowerOff request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) PowerOffResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Restart the operation to restart a virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) Restart(resourceGroupName string, vmName string) (result autorest.Response, ae error) {
	req, err := client.RestartPreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Restart", "Failure preparing request")
	}

	resp, err := client.RestartSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Restart", "Failure sending request")
	}

	result, err = client.RestartResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Restart", "Failure responding to request")
	}

	return
}

// RestartPreparer prepares the Restart request.
func (client VirtualMachinesClient) RestartPreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/restart"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// RestartSender sends the Restart request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) RestartSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK, http.StatusAccepted)
}

// RestartResponder handles the response to the Restart request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) RestartResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Start the operation to start a virtual machine.
//
// resourceGroupName is the name of the resource group. vmName is the name of
// the virtual machine.
func (client VirtualMachinesClient) Start(resourceGroupName string, vmName string) (result autorest.Response, ae error) {
	req, err := client.StartPreparer(resourceGroupName, vmName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Start", "Failure preparing request")
	}

	resp, err := client.StartSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Start", "Failure sending request")
	}

	result, err = client.StartResponder(resp)
	if err != nil {
		ae = autorest.NewErrorWithError(err, "compute/VirtualMachinesClient", "Start", "Failure responding to request")
	}

	return
}

// StartPreparer prepares the Start request.
func (client VirtualMachinesClient) StartPreparer(resourceGroupName string, vmName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": url.QueryEscape(resourceGroupName),
		"subscriptionId":    url.QueryEscape(client.SubscriptionID),
		"vmName":            url.QueryEscape(vmName),
	}

	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	return autorest.Prepare(&http.Request{},
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPath("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Compute/virtualMachines/{vmName}/start"),
		autorest.WithPathParameters(pathParameters),
		autorest.WithQueryParameters(queryParameters))
}

// StartSender sends the Start request. The method will close the
// http.Response Body if it receives an error.
func (client VirtualMachinesClient) StartSender(req *http.Request) (*http.Response, error) {
	return client.Send(req, http.StatusOK, http.StatusAccepted)
}

// StartResponder handles the response to the Start request. The method always
// closes the http.Response Body.
func (client VirtualMachinesClient) StartResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		autorest.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}
