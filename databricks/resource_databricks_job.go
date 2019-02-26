package databricks

import (
	"github.com/cattail/databricks-sdk-go/databricks"
	"github.com/hashicorp/terraform/helper/schema"
	"log"
	"strconv"
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
				Type:          schema.TypeList,
				Optional:      true,
				ConflictsWith: []string{"spark_jar_task", "spark_python_task", "spark_submit_task"},
				MaxItems:      1,
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
				Type:          schema.TypeList,
				Optional:      true,
				ConflictsWith: []string{"notebook_task", "spark_python_task", "spark_submit_task"},
				MaxItems:      1,
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
				Type:          schema.TypeList,
				Optional:      true,
				ConflictsWith: []string{"notebook_task", "spark_jar_task", "spark_submit_task"},
				MaxItems:      1,
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
				Type:          schema.TypeList,
				Optional:      true,
				ConflictsWith: []string{"notebook_task", "spark_jar_task", "spark_python_task"},
				MaxItems:      1,
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
							//ConflictsWith: []string{"egg", "whl", "pypi", "maven", "cran"},
						},
						"egg": {
							Type:     schema.TypeString,
							Optional: true,
							//ConflictsWith: []string{"jar", "whl", "pypi", "maven", "cran"},
						},
						"whl": {
							Type:     schema.TypeString,
							Optional: true,
							//ConflictsWith: []string{"jar", "egg", "pypi", "maven", "cran"},
						},
						"pypi": {
							Type:     schema.TypeList,
							Optional: true,
							//ConflictsWith: []string{"jar", "egg", "whl", "maven", "cran"},
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
							//ConflictsWith: []string{"jar", "egg", "whl", "pypi", "cran"},
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
							//ConflictsWith: []string{"jar", "egg", "whl", "pypi", "maven"},
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
	client := m.(*databricks.APIClient).JobApi

	request := getJobSettings(d)
	logJSON("[DEBUG] Creating job", request)

	resp, _, err := client.CreateJob(nil, request)
	if err != nil {
		return err
	}

	d.SetId(strconv.FormatInt(resp.JobId, 10))

	return resourceDatabricksJobRead(d, m)
}

func resourceDatabricksJobUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).JobApi

	settings := getJobSettings(d)

	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	if err != nil {
		return err
	}

	request := databricks.JobsResetRequest{
		JobId: jobId,
		NewSettings: &settings,
	}
	logJSON("[DEBUG] Updating job", request)

	_, err = client.ResetJob(nil, request)
	if err != nil {
		return err
	}

	return resourceDatabricksJobRead(d, m)
}

func resourceDatabricksJobDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).JobApi

	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	if err != nil {
		return err
	}

	_, err = client.DeleteJob(nil, databricks.JobsDeleteRequest{
		JobId: jobId,
	})
	if err != nil {
		return err
	}

	d.SetId("")

	return nil
}

func resourceDatabricksJobRead(d *schema.ResourceData, m interface{}) error {
	return nil
}

func getJobSettings(d *schema.ResourceData) databricks.JobSettings {
	request := databricks.JobSettings{}

	if v, ok := d.GetOk("new_cluster"); ok {
		newCluster := getJobNewCluster(v.([]interface{}))
		request.NewCluster = &newCluster
	}

	if v, ok := d.GetOk("existing_cluster_id"); ok {
		request.ExistingClusterId = v.(string)
	}

	if v, ok := d.GetOk("notebook_task"); ok {
		notebookTask := resourceDatabricksJobExpandNotebookTask(v.([]interface{}))
		request.NotebookTask = &notebookTask
	}

	if v, ok := d.GetOk("spark_jar_task"); ok {
		sparkJarTask := resourceDatabricksJobExpandSparkJarTask(v.([]interface{}))
		request.SparkJarTask = &sparkJarTask
	}

	if v, ok := d.GetOk("spark_python_task"); ok {
		sparkPythonTask := resourceDatabricksJobExpandSparkPythonTask(v.([]interface{}))
		request.SparkPythonTask = &sparkPythonTask
	}

	if v, ok := d.GetOk("spark_submit_task"); ok {
		sparkSubmitTask := resourceDatabricksJobExpandSparkSubmitTask(v.([]interface{}))
		request.SparkSubmitTask = &sparkSubmitTask
	}

	if v, ok := d.GetOk("name"); ok {
		request.Name = v.(string)
	}

	if v, ok := d.GetOk("libraries"); ok {
		libraries := resourceDatabricksJobExpandLibraries(v.([]interface{}))
		request.Libraries = libraries
	}

	if v, ok := d.GetOk("email_notifications"); ok {
		emailNotifications := resourceDatabricksJobExpandEmailNotifications(v.([]interface{}))
		request.EmailNotifications = &emailNotifications
	}

	if v, ok := d.GetOk("timeout_seconds"); ok {
		request.TimeoutSeconds = int32(v.(int))
	}

	if v, ok := d.GetOk("max_retries"); ok {
		request.MaxRetries = int32(v.(int))
	}

	if v, ok := d.GetOk("min_retry_interval_millis"); ok {
		request.MinRetryIntervalMillis = int32(v.(int))
	}

	if v, ok := d.GetOk("retry_on_timeout"); ok {
		request.RetryOnTimeout = v.(bool)
	}

	if v, ok := d.GetOk("schedule"); ok {
		schedule := resourceDatabricksJobExpandSchedule(v.([]interface{}))
		request.Schedule = &schedule
	}

	if v, ok := d.GetOk("max_concurrent_runs"); ok {
		request.MaxConcurrentRuns = int32(v.(int))
	}

	return request
}

// TODO(Chiyu): getJobNewCluster is almost the same as getNewCluster
func getJobNewCluster(d []interface{}) databricks.NewCluster {
	m := d[0].(map[string]interface{})

	request := databricks.NewCluster{
		SparkVersion: m["spark_version"].(string),
		NodeTypeId:   m["node_type_id"].(string),
	}

	if v, ok := getOK(m, "num_workers"); ok {
		request.NumWorkers = int32(v.(int))
	}

	if v, ok := getOK(m, "autoscale"); ok {
		autoscale := resourceDatabricksClusterExpandAutoscale(v.([]interface{}))
		request.Autoscale = &autoscale
	}

	if v, ok := getOK(m, "cluster_name"); ok {
		request.ClusterName = v.(string)
	}

	if v, ok := getOK(m, "spark_conf"); ok {
		request.SparkConf = toMapString(v)
	}

	if v, ok := getOK(m, "aws_attributes"); ok {
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(v.([]interface{}))
		request.AwsAttributes = &awsAttributes
	}

	if v, ok := getOK(m, "driver_node_type_id"); ok {
		request.DriverNodeTypeId = v.(string)
	}

	if v, ok := getOK(m, "ssh_public_keys"); ok {
		request.SshPublicKeys = toSliceString(v)
	}

	if v, ok := getOK(m, "custom_tags"); ok {
		request.CustomTags = toMapString(v)
	}

	if v, ok := getOK(m, "cluster_log_conf"); ok {
		clusterLogConf := resourceDatabricksClusterExpandClusterLogConf(v.([]interface{}))
		request.ClusterLogConf = &clusterLogConf
	}

	if v, ok := getOK(m, "spark_env_vars"); ok {
		request.SparkEnvVars = toMapString(v)
	}

	if v, ok := getOK(m, "autotermination_minutes"); ok {
		request.AutoterminationMinutes = int32(v.(int))
	}

	if v, ok := getOK(m, "enable_elastic_disk"); ok {
		request.EnableElasticDisk = v.(bool)
	}

	return request
}

func resourceDatabricksJobExpandNotebookTask(d []interface{}) databricks.NotebookTask {
	m := d[0].(map[string]interface{})

	result := databricks.NotebookTask{}

	if v, ok := getOK(m, "notebook_path"); ok {
		result.NotebookPath = v.(string)
	}

	if v, ok := getOK(m, "base_parameters"); ok {
		result.BaseParameters = toSliceMapString(v)
	}

	log.Println("xxxxx", result.BaseParameters)

	return result
}

func resourceDatabricksJobExpandSparkJarTask(d []interface{}) databricks.SparkJarTask {
	m := d[0].(map[string]interface{})

	result := databricks.SparkJarTask{}

	if v, ok := getOK(m, "jar_uri"); ok {
		result.JarUri = v.(string)
	}

	if v, ok := getOK(m, "main_class_name"); ok {
		result.MainClassName = v.(string)
	}

	if v, ok := getOK(m, "parameters"); ok {
		result.Parameters = toSliceString(v)
	}

	return result
}

func resourceDatabricksJobExpandSparkPythonTask(d []interface{}) databricks.SparkPythonTask {
	m := d[0].(map[string]interface{})

	result := databricks.SparkPythonTask{}

	if v, ok := getOK(m, "python_file"); ok {
		result.PythonFile = v.(string)
	}

	if v, ok := getOK(m, "parameters"); ok {
		result.Parameters = toSliceString(v)
	}

	return result
}

func resourceDatabricksJobExpandSparkSubmitTask(d []interface{}) databricks.SparkSubmitTask {
	m := d[0].(map[string]interface{})

	result := databricks.SparkSubmitTask{}

	if v, ok := getOK(m, "parameters"); ok {
		result.Parameters = toSliceString(v)
	}

	return result
}

func resourceDatabricksJobExpandLibraries(d []interface{}) []databricks.Library {
	libraries := make([]databricks.Library, len(d))

	for i, value := range d {
		m := value.(map[string]interface{})
		library := databricks.Library{}

		if v, ok := getOK(m, "jar"); ok {
			library.Jar = v.(string)
		}
		if v, ok := getOK(m, "egg"); ok {
			library.Egg = v.(string)
		}
		if v, ok := getOK(m, "whl"); ok {
			library.Whl = v.(string)
		}
		if v, ok := getOK(m, "pypi"); ok {
			elem := v.([]interface{})[0].(map[string]interface{})
			pypi := databricks.PythonPyPiLibrary{}
			if v, ok := elem["package"]; ok {
				pypi.Package_ = v.(string)
			}
			if v, ok := elem["repo"]; ok {
				pypi.Repo = v.(string)
			}
			library.Pypi = &pypi
		}
		if v, ok := getOK(m, "maven"); ok {
			elem := v.([]interface{})[0].(map[string]interface{})
			maven := databricks.MavenLibrary{}
			if v, ok := elem["coordinates"]; ok {
				maven.Coordinates = v.(string)
			}
			if v, ok := elem["repo"]; ok {
				maven.Repo = v.(string)
			}
			if v, ok := elem["exclusions"]; ok {
				maven.Exclusions = toSliceString(v)
			}
			library.Maven = &maven
		}
		if v, ok := getOK(m, "cran"); ok {
			elem := v.([]interface{})[0].(map[string]interface{})
			cran := databricks.RCranLibrary{}
			if v, ok := elem["package"]; ok {
				cran.Package_ = v.(string)
			}
			if v, ok := elem["repo"]; ok {
				cran.Repo = v.(string)
			}
			library.Cran = &cran
		}

		libraries[i] = library
	}

	return libraries
}

func resourceDatabricksJobExpandEmailNotifications(d []interface{}) databricks.JobEmailNotifications {
	m := d[0].(map[string]interface{})

	result := databricks.JobEmailNotifications{}

	if v, ok := getOK(m, "on_start"); ok {
		result.OnStart = toSliceString(v)
	}

	if v, ok := getOK(m, "on_success"); ok {
		result.OnSuccess = toSliceString(v)
	}

	if v, ok := getOK(m, "on_failure"); ok {
		result.OnFailure = toSliceString(v)
	}

	if v, ok := getOK(m, "no_alert_for_skipped_runs"); ok {
		result.NoAlertForSkippedRuns = v.(bool)
	}

	return result
}

func resourceDatabricksJobExpandSchedule(d []interface{}) databricks.CronSchedule {
	m := d[0].(map[string]interface{})

	result := databricks.CronSchedule{}

	if v, ok := getOK(m, "quartz_cron_expression"); ok {
		result.QuartzCronExpression = v.(string)
	}

	if v, ok := getOK(m, "timezone_id"); ok {
		result.TimezoneId = v.(string)
	}

	return result
}
