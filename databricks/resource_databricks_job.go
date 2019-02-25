package databricks

import (
"github.com/hashicorp/terraform/helper/schema"
)

func resourceDatabricksJob() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabricksJobCreate,
		Read:   resourceDatabricksJobRead,
		Update: resourceDatabricksJobUpdate,
		Delete: resourceDatabricksJobDelete,

		Schema: map[string]*schema.Schema{
			"address": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}

func resourceDatabricksJobCreate(d *schema.ResourceData, m interface{}) error {
	return resourceDatabricksJobRead(d, m)
}

func resourceDatabricksJobRead(d *schema.ResourceData, m interface{}) error {
	return nil
}

func resourceDatabricksJobUpdate(d *schema.ResourceData, m interface{}) error {
	return resourceDatabricksJobRead(d, m)
}

func resourceDatabricksJobDelete(d *schema.ResourceData, m interface{}) error {
	return nil
}
