package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"azure-sdk-for-go/arm/examples/helpers"
	"azure-sdk-for-go/arm/resources/resources"
	"azure-sdk-for-go/arm/storage"

	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/to"

	"try-snap/common"
	"try-snap/controllers"
)

func main() {
	log.SetFlags(log.Lshortfile)
	resourceGroup := "tomrg-cluster-9e95d8a8"
	name := "gosdktestname01"

	c := map[string]string{
		"AZURE_CLIENT_ID":       os.Getenv("AZURE_CLIENT_ID"),
		"AZURE_CLIENT_SECRET":   os.Getenv("AZURE_CLIENT_SECRET"),
		"AZURE_SUBSCRIPTION_ID": os.Getenv("AZURE_SUBSCRIPTION_ID"),
		"AZURE_TENANT_ID":       os.Getenv("AZURE_TENANT_ID"),

		"clientID":       os.Getenv("AZURE_CLIENT_ID"),
		"clientSecret":   os.Getenv("AZURE_CLIENT_SECRET"),
		"subscriptionID": os.Getenv("AZURE_SUBSCRIPTION_ID"),
		"tenantID":       os.Getenv("AZURE_TENANT_ID"),
	}
	if err := checkEnvVar(&c); err != nil {
		log.Fatalf("Error: %v", err)
		return
	}
	spt, err := helpers.NewServicePrincipalTokenFromCredentials(c, azure.PublicCloud.ResourceManagerEndpoint)
	if err != nil {
		log.Fatalf("Error: %v", err)
		return
	}

	ac := storage.NewAccountsClient(c["AZURE_SUBSCRIPTION_ID"])
	ac.Authorizer = spt

	cna, err := ac.CheckNameAvailability(
		storage.AccountCheckNameAvailabilityParameters{
			Name: to.StringPtr(name),
			Type: to.StringPtr("Microsoft.Storage/storageAccounts")})
	if err != nil {
		log.Fatalf("Error: %v", err)
		return
	}
	if !to.Bool(cna.NameAvailable) {
		fmt.Printf("%s is unavailable -- try with another name\n", name)
		return
	}
	fmt.Printf("%s is available\n\n", name)

	//////////////

	log.Printf("================================\n")
	log.Printf("================================\n")
	log.Printf("================================\n")
	log.Printf("================================\n")

	rc := resources.NewGroupsClient(c["AZURE_SUBSCRIPTION_ID"])
	rc.Authorizer = spt

	rsp, err := rc.ListResources(resourceGroup, "", "", nil)
	if err != nil {
		fmt.Printf("List Resources of '%s' failed with status %s\n...%v\n", resourceGroup, rsp.Status, err)
		return
	}

	log.Printf("rsp = %T - %+v", rsp, rsp)
	log.Printf("rsp.Response = %T - %+v", rsp.Response, rsp.Response)
	log.Printf("rsp.Value = %T - %+v", rsp.Value, rsp.Value)

	rscs := *rsp.Value
	for i, rsc := range rscs {
		//log.Printf("rsc[%d] = %T - %+v", i, rsc, rsc)
		//log.Printf(" name = %+v  type = %+v  location = %+v  kind = %+v  sku = %+v", *rsc.Name, *rsc.Type, *rsc.Location, *rsc.Kind, *rsc.Sku)
		log.Printf("rsc[%v]: name = %+v  type = %+v  location = %+v", i, *rsc.Name, *rsc.Type, *rsc.Location)
	}

	ctx := context.Background()
	logr := log.New(os.Stdout, "stdoutLogger", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	// Global context
	ctx = context.WithValue(ctx, common.LoggerKey, logr)
	//ctx = context.WithValue(ctx, common.DbhKey, common.Dbh())
	//ctx = context.WithValue(ctx, common.SqlQueriesKey, common.Config().SqlQueries)
	ctx = context.WithValue(ctx, common.CloudInfoKey, common.Config().CloudInfo)

	// Per request context
	ctx = context.WithValue(ctx, common.SystemRequestIDKey, common.SystemRequestIDType("SystemID"))
	ctx = context.WithValue(ctx, common.UserRequestIDKey, "UserID")

	creds := controllers.NewAzCredentials(ctx, c)
	gc, err := controllers.NewGroupsClient(ctx, creds)
	if err != nil {
		log.Printf("unable to create azure resource group query connection, %v", err)
		return
	}

	rsp, err = gc.ListResources(resourceGroup, "", "", nil)
	if err != nil {
		fmt.Printf("List Resources of '%s' failed with status %s\n...%v\n", resourceGroup, rsp.Status, err)
		return
	}

	log.Printf("rsp = %T - %+v", rsp, rsp)
	log.Printf("rsp.Response = %T - %+v", rsp.Response, rsp.Response)
	log.Printf("rsp.Value = %T - %+v", rsp.Value, rsp.Value)

	rscs = *rsp.Value
	for i, rsc := range rscs {
		//log.Printf("rsc[%d] = %T - %+v", i, rsc, rsc)
		//log.Printf(" name = %+v  type = %+v  location = %+v  kind = %+v  sku = %+v", *rsc.Name, *rsc.Type, *rsc.Location, *rsc.Kind, *rsc.Sku)
		log.Printf("rsc[%v]: name = %+v  type = %+v  location = %+v", i, *rsc.Name, *rsc.Type, *rsc.Location)
	}

	panic("bye")
	///////////////////

	cp := storage.AccountCreateParameters{
		Sku: &storage.Sku{
			Name: storage.StandardLRS,
			//Tier: storage.Standard
		},
		Location: to.StringPtr("westus")}
	cancel := make(chan struct{})
	if _, err = ac.Create(resourceGroup, name, cp, cancel); err != nil {
		fmt.Printf("Create '%s' storage account failed: %v\n", name, err)
		return
	}
	fmt.Printf("Successfully created '%s' storage account in '%s' resource group\n\n", name, resourceGroup)

	r, err := ac.Delete(resourceGroup, name)
	if err != nil {
		fmt.Printf("Delete of '%s' failed with status %s\n...%v\n", name, r.Status, err)
		return
	}
	fmt.Printf("Deletion of '%s' storage account in '%s' resource group succeeded -- %s\n", name, resourceGroup, r.Status)
}

func checkEnvVar(envVars *map[string]string) error {
	var missingVars []string
	for varName, value := range *envVars {
		if value == "" {
			missingVars = append(missingVars, varName)
		}
	}
	if len(missingVars) > 0 {
		return fmt.Errorf("Missing environment variables %v", missingVars)
	}
	return nil
}
