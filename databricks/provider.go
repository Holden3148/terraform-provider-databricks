package databricks

import (
	"github.com/cattail/databricks-sdk-go/databricks"
	"github.com/hashicorp/terraform/helper/schema"
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"domain": {
				Type:     schema.TypeString,
				Required: true,
			},
			"token": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"databricks_cluster": resourceDatabricksCluster(),
		},
		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	cfg := databricks.NewConfiguration()
	cfg.AddDefaultHeader("Authorization", "Bearer "+d.Get("token").(string))
	cfg.BasePath = d.Get("domain").(string) + "/api/2.0"
	client := databricks.NewAPIClient(cfg)
	return client, nil
}
