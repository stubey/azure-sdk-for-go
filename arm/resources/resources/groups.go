package resources

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
	"log"
	"net/http"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/validation"
)

// GroupsClient is the provides operations for working with resources and
// resource groups.
type GroupsClient struct {
	ManagementClient
}

// NewGroupsClient creates an instance of the GroupsClient client.
func NewGroupsClient(subscriptionID string) GroupsClient {
	return NewGroupsClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewGroupsClientWithBaseURI creates an instance of the GroupsClient client.
func NewGroupsClientWithBaseURI(baseURI string, subscriptionID string) GroupsClient {
	return GroupsClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// CheckExistence checks whether a resource group exists.
//
// resourceGroupName is the name of the resource group to check. The name is
// case insensitive.
func (client GroupsClient) CheckExistence(resourceGroupName string) (result autorest.Response, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "CheckExistence")
	}

	req, err := client.CheckExistencePreparer(resourceGroupName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "CheckExistence", nil, "Failure preparing request")
	}

	resp, err := client.CheckExistenceSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "CheckExistence", resp, "Failure sending request")
	}

	result, err = client.CheckExistenceResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "CheckExistence", resp, "Failure responding to request")
	}

	return
}

// CheckExistencePreparer prepares the CheckExistence request.
func (client GroupsClient) CheckExistencePreparer(resourceGroupName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsHead(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups/{resourceGroupName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// CheckExistenceSender sends the CheckExistence request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) CheckExistenceSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// CheckExistenceResponder handles the response to the CheckExistence request. The method always
// closes the http.Response Body.
func (client GroupsClient) CheckExistenceResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusNoContent, http.StatusNotFound),
		autorest.ByClosing())
	result.Response = resp
	return
}

// CreateOrUpdate creates a resource group.
//
// resourceGroupName is the name of the resource group to create or update.
// parameters is parameters supplied to the create or update a resource
// group.
func (client GroupsClient) CreateOrUpdate(resourceGroupName string, parameters ResourceGroup) (result ResourceGroup, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}},
		{TargetValue: parameters,
			Constraints: []validation.Constraint{{Target: "parameters.Properties", Name: validation.Null, Rule: false,
				Chain: []validation.Constraint{{Target: "parameters.Properties.ProvisioningState", Name: validation.ReadOnly, Rule: true, Chain: nil}}},
				{Target: "parameters.Location", Name: validation.Null, Rule: true, Chain: nil},
				{Target: "parameters.ID", Name: validation.ReadOnly, Rule: true, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "CreateOrUpdate")
	}

	req, err := client.CreateOrUpdatePreparer(resourceGroupName, parameters)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "CreateOrUpdate", nil, "Failure preparing request")
	}

	resp, err := client.CreateOrUpdateSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "CreateOrUpdate", resp, "Failure sending request")
	}

	result, err = client.CreateOrUpdateResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "CreateOrUpdate", resp, "Failure responding to request")
	}

	return
}

// CreateOrUpdatePreparer prepares the CreateOrUpdate request.
func (client GroupsClient) CreateOrUpdatePreparer(resourceGroupName string, parameters ResourceGroup) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsJSON(),
		autorest.AsPut(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups/{resourceGroupName}", pathParameters),
		autorest.WithJSON(parameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// CreateOrUpdateSender sends the CreateOrUpdate request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) CreateOrUpdateSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// CreateOrUpdateResponder handles the response to the CreateOrUpdate request. The method always
// closes the http.Response Body.
func (client GroupsClient) CreateOrUpdateResponder(resp *http.Response) (result ResourceGroup, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusCreated, http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// Delete when you delete a resource group, all of its resources are also
// deleted. Deleting a resource group deletes all of its template deployments
// and currently stored operations. This method may poll for completion.
// Polling can be canceled by passing the cancel channel argument. The
// channel will be used to cancel polling and any outstanding HTTP requests.
//
// resourceGroupName is the name of the resource group to delete. The name is
// case insensitive.
func (client GroupsClient) Delete(resourceGroupName string, cancel <-chan struct{}) (result autorest.Response, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "Delete")
	}

	req, err := client.DeletePreparer(resourceGroupName, cancel)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "Delete", nil, "Failure preparing request")
	}

	resp, err := client.DeleteSender(req)
	if err != nil {
		result.Response = resp
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "Delete", resp, "Failure sending request")
	}

	result, err = client.DeleteResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "Delete", resp, "Failure responding to request")
	}

	return
}

// DeletePreparer prepares the Delete request.
func (client GroupsClient) DeletePreparer(resourceGroupName string, cancel <-chan struct{}) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsDelete(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups/{resourceGroupName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{Cancel: cancel})
}

// DeleteSender sends the Delete request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) DeleteSender(req *http.Request) (*http.Response, error) {
	// return autorest.SendWithSender(client,
	// 	req,
	// 	azure.DoPollForAsynchronous(client.PollingDelay))
	return autorest.SendWithSender(client,
		req,
	)
}

// DeleteResponder handles the response to the Delete request. The method always
// closes the http.Response Body.
func (client GroupsClient) DeleteResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusAccepted, http.StatusOK),
		autorest.ByClosing())
	result.Response = resp
	return
}

// ExportTemplate captures the specified resource group as a template.
//
// resourceGroupName is the name of the resource group to export as a
// template. parameters is parameters for exporting the template.
func (client GroupsClient) ExportTemplate(resourceGroupName string, parameters ExportTemplateRequest) (result ResourceGroupExportResult, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "ExportTemplate")
	}

	req, err := client.ExportTemplatePreparer(resourceGroupName, parameters)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "ExportTemplate", nil, "Failure preparing request")
	}

	resp, err := client.ExportTemplateSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "ExportTemplate", resp, "Failure sending request")
	}

	result, err = client.ExportTemplateResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "ExportTemplate", resp, "Failure responding to request")
	}

	return
}

// ExportTemplatePreparer prepares the ExportTemplate request.
func (client GroupsClient) ExportTemplatePreparer(resourceGroupName string, parameters ExportTemplateRequest) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsJSON(),
		autorest.AsPost(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups/{resourceGroupName}/exportTemplate", pathParameters),
		autorest.WithJSON(parameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// ExportTemplateSender sends the ExportTemplate request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) ExportTemplateSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// ExportTemplateResponder handles the response to the ExportTemplate request. The method always
// closes the http.Response Body.
func (client GroupsClient) ExportTemplateResponder(resp *http.Response) (result ResourceGroupExportResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// Get gets a resource group.
//
// resourceGroupName is the name of the resource group to get. The name is
// case insensitive.
func (client GroupsClient) Get(resourceGroupName string) (result ResourceGroup, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "Get")
	}

	req, err := client.GetPreparer(resourceGroupName)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "Get", nil, "Failure preparing request")
	}

	resp, err := client.GetSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "Get", resp, "Failure sending request")
	}

	result, err = client.GetResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "Get", resp, "Failure responding to request")
	}

	return
}

// GetPreparer prepares the Get request.
func (client GroupsClient) GetPreparer(resourceGroupName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups/{resourceGroupName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// GetSender sends the Get request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) GetSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// GetResponder handles the response to the Get request. The method always
// closes the http.Response Body.
func (client GroupsClient) GetResponder(resp *http.Response) (result ResourceGroup, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// List gets all the resource groups for a subscription.
//
// filter is the filter to apply on the operation. top is the number of
// results to return. If null is passed, returns all resource groups.
func (client GroupsClient) List(filter string, top *int32) (result ResourceGroupListResult, err error) {
	req, err := client.ListPreparer(filter, top)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "List", nil, "Failure preparing request")
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "List", resp, "Failure sending request")
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "List", resp, "Failure responding to request")
	}

	return
}

// ListPreparer prepares the List request.
func (client GroupsClient) ListPreparer(filter string, top *int32) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"subscriptionId": autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}
	if len(filter) > 0 {
		queryParameters["$filter"] = autorest.Encode("query", filter)
	}
	if top != nil {
		queryParameters["$top"] = autorest.Encode("query", *top)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// ListSender sends the List request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) ListSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// ListResponder handles the response to the List request. The method always
// closes the http.Response Body.
func (client GroupsClient) ListResponder(resp *http.Response) (result ResourceGroupListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListNextResults retrieves the next set of results, if any.
func (client GroupsClient) ListNextResults(lastResults ResourceGroupListResult) (result ResourceGroupListResult, err error) {
	req, err := lastResults.ResourceGroupListResultPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "List", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "List", resp, "Failure sending next results request")
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "List", resp, "Failure responding to next results request")
	}

	return
}

// ListResources get all the resources for a resource group.
//
// resourceGroupName is the resource group with the resources to get. filter
// is the filter to apply on the operation. expand is the $expand query
// parameter top is the number of results to return. If null is passed,
// returns all resources.
func (client GroupsClient) ListResources(resourceGroupName string, filter string, expand string, top *int32) (result ResourceListResult, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "ListResources")
	}

	req, err := client.ListResourcesPreparer(resourceGroupName, filter, expand, top)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "ListResources", nil, "Failure preparing request")
	}

	log.Printf("TRACE: req = %+v", req)

	resp, err := client.ListResourcesSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "ListResources", resp, "Failure sending request")
	}

	log.Printf("TRACE: resp = %+v", resp)

	result, err = client.ListResourcesResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "ListResources", resp, "Failure responding to request")
	}

	log.Printf("TRACE: result = %+v", result)

	return
}

// ListResourcesPreparer prepares the ListResources request.
func (client GroupsClient) ListResourcesPreparer(resourceGroupName string, filter string, expand string, top *int32) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}
	if len(filter) > 0 {
		queryParameters["$filter"] = autorest.Encode("query", filter)
	}
	if len(expand) > 0 {
		queryParameters["$expand"] = autorest.Encode("query", expand)
	}
	if top != nil {
		queryParameters["$top"] = autorest.Encode("query", *top)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/resources", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// ListResourcesSender sends the ListResources request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) ListResourcesSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// ListResourcesResponder handles the response to the ListResources request. The method always
// closes the http.Response Body.
func (client GroupsClient) ListResourcesResponder(resp *http.Response) (result ResourceListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListResourcesNextResults retrieves the next set of results, if any.
func (client GroupsClient) ListResourcesNextResults(lastResults ResourceListResult) (result ResourceListResult, err error) {
	req, err := lastResults.ResourceListResultPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "ListResources", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}

	resp, err := client.ListResourcesSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "ListResources", resp, "Failure sending next results request")
	}

	result, err = client.ListResourcesResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "ListResources", resp, "Failure responding to next results request")
	}

	return
}

// Patch resource groups can be updated through a simple PATCH operation to a
// group address. The format of the request is the same as that for creating
// a resource group. If a field is unspecified, the current value is retained.
//
// resourceGroupName is the name of the resource group to update. The name is
// case insensitive. parameters is parameters supplied to update a resource
// group.
func (client GroupsClient) Patch(resourceGroupName string, parameters ResourceGroup) (result ResourceGroup, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[-\w\._\(\)]+$`, Chain: nil}}},
		{TargetValue: parameters,
			Constraints: []validation.Constraint{{Target: "parameters.ID", Name: validation.ReadOnly, Rule: true, Chain: nil}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "resources.GroupsClient", "Patch")
	}

	req, err := client.PatchPreparer(resourceGroupName, parameters)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "Patch", nil, "Failure preparing request")
	}

	resp, err := client.PatchSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "resources.GroupsClient", "Patch", resp, "Failure sending request")
	}

	result, err = client.PatchResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "resources.GroupsClient", "Patch", resp, "Failure responding to request")
	}

	return
}

// PatchPreparer prepares the Patch request.
func (client GroupsClient) PatchPreparer(resourceGroupName string, parameters ResourceGroup) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	queryParameters := map[string]interface{}{
		"api-version": client.APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsJSON(),
		autorest.AsPatch(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourcegroups/{resourceGroupName}", pathParameters),
		autorest.WithJSON(parameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// PatchSender sends the Patch request. The method will close the
// http.Response Body if it receives an error.
func (client GroupsClient) PatchSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// PatchResponder handles the response to the Patch request. The method always
// closes the http.Response Body.
func (client GroupsClient) PatchResponder(resp *http.Response) (result ResourceGroup, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}
