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
			"new_cluster": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: resourceDatabricksCluster().Schema,
				},
				ConflictsWith: []string{"existing_cluster_id"},
			},
			"existing_cluster_id": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"new_cluster"},
			},
			"notebook_task": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"notebook_path": {
							Type:     schema.TypeString,
							Required: true,
						},
						"base_parameters": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeMap,
								Elem: &schema.Schema{Type: schema.TypeString},
							},
						},
					},
				},
			},
			"spark_jar_task": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"jar_uri": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"main_class_name": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"parameters": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
					},
				},
			},
			"spark_python_task": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"python_file": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"parameters": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
					},
				},
			},
			"spark_submit_task": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"parameters": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
					},
				},
			},
			"name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"libraries": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"jar": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"egg": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"whl": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"pypi": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"package": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"repo": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
						"maven": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"coordinates": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"repo": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"exclusions": {
										Type:     schema.TypeList,
										Optional: true,
										Elem:     &schema.Schema{Type: schema.TypeString},
									},
								},
							},
						},
						"cran": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"package": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"repo": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
					},
				},
			},
			"email_notifications": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"on_start": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"on_success": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"on_failure": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"no_alert_for_skipped_runs": {
							Type:     schema.TypeBool,
							Optional: true,
						},
					},
				},
			},
			"timeout_seconds": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"max_retries": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"min_retry_interval_millis": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"retry_on_timeout": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"schedule": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"quartz_cron_expression": {
							Type:     schema.TypeString,
							Required: true,
						},
						"timezone_id": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
			"max_concurrent_runs": {
				Type:     schema.TypeInt,
				Optional: true,
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
