package apimanagement

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
// Code generated by Microsoft (R) AutoRest Code Generator 1.0.1.0
// Changes may cause incorrect behavior and will be lost if the code is
// regenerated.

import (
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/validation"
	"net/http"
)

// UserSubscriptionsClient is the use these REST APIs for performing operations
// on entities like API, Product, and Subscription associated with your Azure
// API Management deployment.
type UserSubscriptionsClient struct {
	ManagementClient
}

// NewUserSubscriptionsClient creates an instance of the
// UserSubscriptionsClient client.
func NewUserSubscriptionsClient(subscriptionID string) UserSubscriptionsClient {
	return NewUserSubscriptionsClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewUserSubscriptionsClientWithBaseURI creates an instance of the
// UserSubscriptionsClient client.
func NewUserSubscriptionsClientWithBaseURI(baseURI string, subscriptionID string) UserSubscriptionsClient {
	return UserSubscriptionsClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// ListByUser lists the collection of subscriptions of the specified user.
//
// resourceGroupName is the name of the resource group. serviceName is the name
// of the API Management service. uid is user identifier. Must be unique in the
// current API Management service instance. filter is | Field        |
// Supported operators    | Supported functions                         |
// |--------------|------------------------|---------------------------------------------|
// | id           | ge, le, eq, ne, gt, lt | substringof, contains, startswith,
// endswith |
// | name         | ge, le, eq, ne, gt, lt | substringof, contains, startswith,
// endswith |
// | stateComment | ge, le, eq, ne, gt, lt | substringof, contains, startswith,
// endswith |
// | userId       | ge, le, eq, ne, gt, lt | substringof, contains, startswith,
// endswith |
// | productId    | ge, le, eq, ne, gt, lt | substringof, contains, startswith,
// endswith |
// | state        | eq                     |
// | top is number of records to return. skip is number of records to skip.
func (client UserSubscriptionsClient) ListByUser(resourceGroupName string, serviceName string, uid string, filter string, top *int32, skip *int32) (result SubscriptionCollection, err error) {
	if err := validation.Validate([]validation.Validation{
		{TargetValue: serviceName,
			Constraints: []validation.Constraint{{Target: "serviceName", Name: validation.MaxLength, Rule: 50, Chain: nil},
				{Target: "serviceName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "serviceName", Name: validation.Pattern, Rule: `^[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$`, Chain: nil}}},
		{TargetValue: uid,
			Constraints: []validation.Constraint{{Target: "uid", Name: validation.MaxLength, Rule: 256, Chain: nil},
				{Target: "uid", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "uid", Name: validation.Pattern, Rule: `^[^*#&+:<>?]+$`, Chain: nil}}},
		{TargetValue: top,
			Constraints: []validation.Constraint{{Target: "top", Name: validation.Null, Rule: false,
				Chain: []validation.Constraint{{Target: "top", Name: validation.InclusiveMinimum, Rule: 1, Chain: nil}}}}},
		{TargetValue: skip,
			Constraints: []validation.Constraint{{Target: "skip", Name: validation.Null, Rule: false,
				Chain: []validation.Constraint{{Target: "skip", Name: validation.InclusiveMinimum, Rule: 0, Chain: nil}}}}}}); err != nil {
		return result, validation.NewErrorWithValidationError(err, "apimanagement.UserSubscriptionsClient", "ListByUser")
	}

	req, err := client.ListByUserPreparer(resourceGroupName, serviceName, uid, filter, top, skip)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "apimanagement.UserSubscriptionsClient", "ListByUser", nil, "Failure preparing request")
	}

	resp, err := client.ListByUserSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "apimanagement.UserSubscriptionsClient", "ListByUser", resp, "Failure sending request")
	}

	result, err = client.ListByUserResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "apimanagement.UserSubscriptionsClient", "ListByUser", resp, "Failure responding to request")
	}

	return
}

// ListByUserPreparer prepares the ListByUser request.
func (client UserSubscriptionsClient) ListByUserPreparer(resourceGroupName string, serviceName string, uid string, filter string, top *int32, skip *int32) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"serviceName":       autorest.Encode("path", serviceName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
		"uid":               autorest.Encode("path", uid),
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
	if skip != nil {
		queryParameters["$skip"] = autorest.Encode("query", *skip)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.ApiManagement/service/{serviceName}/users/{uid}/subscriptions", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare(&http.Request{})
}

// ListByUserSender sends the ListByUser request. The method will close the
// http.Response Body if it receives an error.
func (client UserSubscriptionsClient) ListByUserSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req)
}

// ListByUserResponder handles the response to the ListByUser request. The method always
// closes the http.Response Body.
func (client UserSubscriptionsClient) ListByUserResponder(resp *http.Response) (result SubscriptionCollection, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListByUserNextResults retrieves the next set of results, if any.
func (client UserSubscriptionsClient) ListByUserNextResults(lastResults SubscriptionCollection) (result SubscriptionCollection, err error) {
	req, err := lastResults.SubscriptionCollectionPreparer()
	if err != nil {
		return result, autorest.NewErrorWithError(err, "apimanagement.UserSubscriptionsClient", "ListByUser", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}

	resp, err := client.ListByUserSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "apimanagement.UserSubscriptionsClient", "ListByUser", resp, "Failure sending next results request")
	}

	result, err = client.ListByUserResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "apimanagement.UserSubscriptionsClient", "ListByUser", resp, "Failure responding to next results request")
	}

	return
}
