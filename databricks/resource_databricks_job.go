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
				Default:  1,
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
		JobId:       jobId,
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
	client := m.(*databricks.APIClient).JobApi

	jobId, err := strconv.ParseInt(d.Id(), 10, 64)
	if err != nil {
		return err
	}

	resp, httpResponse, err := client.GetJob(nil, jobId)
	if err != nil {
		if resourceDatabricksClusterNotExistsError(httpResponse) {
			log.Printf("[WARN] Job (%s) not found, removing from state", d.Id())
			d.SetId("")
			return nil
		}
		return err
	}

	return setJobSettings(d, *resp.Settings)
}

func getJobSettings(d *schema.ResourceData) databricks.JobSettings {
	jobSettings := databricks.JobSettings{}

	if v, ok := d.GetOk("new_cluster"); ok {
		newCluster := getClusterSettings(v.([]interface{})[0])
		jobSettings.NewCluster = &newCluster
	}

	if v, ok := d.GetOk("existing_cluster_id"); ok {
		jobSettings.ExistingClusterId = v.(string)
	}

	if v, ok := d.GetOk("notebook_task"); ok {
		notebookTask := resourceDatabricksJobExpandNotebookTask(v.([]interface{}))
		jobSettings.NotebookTask = &notebookTask
	}

	if v, ok := d.GetOk("spark_jar_task"); ok {
		sparkJarTask := resourceDatabricksJobExpandSparkJarTask(v.([]interface{}))
		jobSettings.SparkJarTask = &sparkJarTask
	}

	if v, ok := d.GetOk("spark_python_task"); ok {
		sparkPythonTask := resourceDatabricksJobExpandSparkPythonTask(v.([]interface{}))
		jobSettings.SparkPythonTask = &sparkPythonTask
	}

	if v, ok := d.GetOk("spark_submit_task"); ok {
		sparkSubmitTask := resourceDatabricksJobExpandSparkSubmitTask(v.([]interface{}))
		jobSettings.SparkSubmitTask = &sparkSubmitTask
	}

	if v, ok := d.GetOk("name"); ok {
		jobSettings.Name = v.(string)
	}

	if v, ok := d.GetOk("libraries"); ok {
		libraries := resourceDatabricksJobExpandLibraries(v.([]interface{}))
		jobSettings.Libraries = libraries
	}

	if v, ok := d.GetOk("email_notifications"); ok {
		emailNotifications := resourceDatabricksJobExpandEmailNotifications(v.([]interface{}))
		jobSettings.EmailNotifications = &emailNotifications
	}

	if v, ok := d.GetOk("timeout_seconds"); ok {
		jobSettings.TimeoutSeconds = int32(v.(int))
	}

	if v, ok := d.GetOk("max_retries"); ok {
		jobSettings.MaxRetries = int32(v.(int))
	}

	if v, ok := d.GetOk("min_retry_interval_millis"); ok {
		jobSettings.MinRetryIntervalMillis = int32(v.(int))
	}

	if v, ok := d.GetOk("retry_on_timeout"); ok {
		jobSettings.RetryOnTimeout = v.(bool)
	}

	if v, ok := d.GetOk("schedule"); ok {
		schedule := resourceDatabricksJobExpandSchedule(v.([]interface{}))
		jobSettings.Schedule = &schedule
	}

	if v, ok := d.GetOk("max_concurrent_runs"); ok {
		jobSettings.MaxConcurrentRuns = int32(v.(int))
	}

	return jobSettings
}

func setJobSettings(d interface{}, jobSettings databricks.JobSettings) error {
	if jobSettings.NewCluster != nil {
		m := make(map[string]interface{})
		err := setClusterSettings(m, *jobSettings.NewCluster)
		if err != nil {
			return err
		}

		err = set(d, "new_cluster", []map[string]interface{}{m})
		if err != nil {
			return err
		}
	}

	err := set(d, "existing_cluster_id", jobSettings.ExistingClusterId)
	if err != nil {
		return err
	}

	err = set(d, "notebook_task", resourceDatabricksJobFlattenNotebookTask(jobSettings.NotebookTask))
	if err != nil {
		return err
	}

	err = set(d, "spark_jar_task", resourceDatabricksJobFlattenSparkJarTask(jobSettings.SparkJarTask))
	if err != nil {
		return err
	}

	err = set(d, "spark_python_task", resourceDatabricksJobFlattenSparkPythonTask(jobSettings.SparkPythonTask))
	if err != nil {
		return err
	}

	err = set(d, "spark_submit_task", resourceDatabricksJobFlattenSparkSubmitTask(jobSettings.SparkSubmitTask))
	if err != nil {
		return err
	}

	err = set(d, "name", jobSettings.Name)
	if err != nil {
		return err
	}

	err = set(d, "libraries", resourceDatabricksJobFlattenLibraries(jobSettings.Libraries))
	if err != nil {
		return err
	}

	err = set(d, "email_notifications", resourceDatabricksJobFlattenEmailNotification(jobSettings.EmailNotifications))
	if err != nil {
		return err
	}

	err = set(d, "timeout_seconds", jobSettings.TimeoutSeconds)
	if err != nil {
		return err
	}

	err = set(d, "max_retries", jobSettings.MaxRetries)
	if err != nil {
		return err
	}

	err = set(d, "min_retry_interval_millis", jobSettings.MinRetryIntervalMillis)
	if err != nil {
		return err
	}

	err = set(d, "retry_on_timeout", jobSettings.RetryOnTimeout)
	if err != nil {
		return err
	}

	err = set(d, "schedule", resourceDatabricksJobFlattenSchedule(jobSettings.Schedule))
	if err != nil {
		return err
	}

	err = set(d, "max_concurrent_runs", jobSettings.MaxConcurrentRuns)
	if err != nil {
		return err
	}

	return nil
}

func resourceDatabricksJobExpandNotebookTask(d []interface{}) databricks.NotebookTask {
	m := d[0].(map[string]interface{})

	result := databricks.NotebookTask{}

	if v, ok := getOk(m, "notebook_path"); ok {
		result.NotebookPath = v.(string)
	}

	if v, ok := getOk(m, "base_parameters"); ok {
		result.BaseParameters = toSliceMapString(v)
	}

	return result
}

func resourceDatabricksJobFlattenNotebookTask(notebookTask *databricks.NotebookTask) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	if notebookTask != nil {
		item := make(map[string]interface{})
		item["notebook_path"] = notebookTask.NotebookPath
		//iBaseParameters := make([]map[string]interface{}, len(notebookTask.BaseParameters))
		//for i, parameters := range notebookTask.BaseParameters {
		//	iBaseParameters[i] = parameters.(map[string]interface{})
		//}
		item["base_parameters"] = notebookTask.BaseParameters
		result = append(result, item)
	}

	return result
}

func resourceDatabricksJobExpandSparkJarTask(d []interface{}) databricks.SparkJarTask {
	m := d[0].(map[string]interface{})

	result := databricks.SparkJarTask{}

	if v, ok := getOk(m, "jar_uri"); ok {
		result.JarUri = v.(string)
	}

	if v, ok := getOk(m, "main_class_name"); ok {
		result.MainClassName = v.(string)
	}

	if v, ok := getOk(m, "parameters"); ok {
		result.Parameters = toSliceString(v)
	}

	return result
}

func resourceDatabricksJobFlattenSparkJarTask(sparkJarTask *databricks.SparkJarTask) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	if sparkJarTask != nil {
		item := make(map[string]interface{})
		item["jar_uri"] = sparkJarTask.JarUri
		item["main_class_name"] = sparkJarTask.MainClassName
		item["parameters"] = sparkJarTask.Parameters
		result = append(result, item)
	}

	return result
}

func resourceDatabricksJobExpandSparkPythonTask(d []interface{}) databricks.SparkPythonTask {
	m := d[0].(map[string]interface{})

	result := databricks.SparkPythonTask{}

	if v, ok := getOk(m, "python_file"); ok {
		result.PythonFile = v.(string)
	}

	if v, ok := getOk(m, "parameters"); ok {
		result.Parameters = toSliceString(v)
	}

	return result
}

func resourceDatabricksJobFlattenSparkPythonTask(sparkPythonTask *databricks.SparkPythonTask) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	if sparkPythonTask != nil {
		item := make(map[string]interface{})
		item["python_file"] = sparkPythonTask.PythonFile
		item["parameters"] = sparkPythonTask.Parameters
		result = append(result, item)
	}

	return result
}

func resourceDatabricksJobExpandSparkSubmitTask(d []interface{}) databricks.SparkSubmitTask {
	m := d[0].(map[string]interface{})

	result := databricks.SparkSubmitTask{}

	if v, ok := getOk(m, "parameters"); ok {
		result.Parameters = toSliceString(v)
	}

	return result
}

func resourceDatabricksJobFlattenSparkSubmitTask(sparkSubmitTask *databricks.SparkSubmitTask) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	if sparkSubmitTask != nil {
		item := make(map[string]interface{})
		item["parameters"] = sparkSubmitTask.Parameters
		result = append(result, item)
	}

	return result
}

func resourceDatabricksJobExpandLibraries(d []interface{}) []databricks.Library {
	libraries := make([]databricks.Library, len(d))

	for i, value := range d {
		m := value.(map[string]interface{})
		library := databricks.Library{}

		if v, ok := getOk(m, "jar"); ok {
			library.Jar = v.(string)
		}
		if v, ok := getOk(m, "egg"); ok {
			library.Egg = v.(string)
		}
		if v, ok := getOk(m, "whl"); ok {
			library.Whl = v.(string)
		}
		if v, ok := getOk(m, "pypi"); ok {
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
		if v, ok := getOk(m, "maven"); ok {
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
		if v, ok := getOk(m, "cran"); ok {
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

func resourceDatabricksJobFlattenLibraries(libraries []databricks.Library) []map[string]interface{} {
	result := make([]map[string]interface{}, len(libraries))

	for i, library := range libraries {
		item := make(map[string]interface{})
		item["jar"] = library.Jar
		item["egg"] = library.Egg
		item["whl"] = library.Whl
		if library.Pypi != nil {
			pypi := make(map[string]interface{})
			pypi["package"] = library.Pypi.Package_
			pypi["repo"] = library.Pypi.Repo
			item["pypi"] = []interface{}{pypi}
		}
		if library.Maven != nil {
			maven := make(map[string]interface{})
			maven["coordinates"] = library.Maven.Coordinates
			maven["repo"] = library.Maven.Repo
			maven["exclusions"] = library.Maven.Exclusions
			item["maven"] = []interface{}{maven}
		}
		if library.Cran != nil {
			cran := make(map[string]interface{})
			cran["package"] = library.Cran.Package_
			cran["repo"] = library.Cran.Repo
			item["pypi"] = []interface{}{cran}
		}
		result[i] = item
	}

	return result
}

func resourceDatabricksJobExpandEmailNotifications(d []interface{}) databricks.JobEmailNotifications {
	m := d[0].(map[string]interface{})

	result := databricks.JobEmailNotifications{}

	if v, ok := getOk(m, "on_start"); ok {
		result.OnStart = toSliceString(v)
	}

	if v, ok := getOk(m, "on_success"); ok {
		result.OnSuccess = toSliceString(v)
	}

	if v, ok := getOk(m, "on_failure"); ok {
		result.OnFailure = toSliceString(v)
	}

	if v, ok := getOk(m, "no_alert_for_skipped_runs"); ok {
		result.NoAlertForSkippedRuns = v.(bool)
	}

	return result
}

func resourceDatabricksJobFlattenEmailNotification(emailNotification *databricks.JobEmailNotifications) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	if emailNotification != nil {
		item := make(map[string]interface{})
		item["on_start"] = emailNotification.OnStart
		item["on_success"] = emailNotification.OnSuccess
		item["on_failure"] = emailNotification.OnFailure
		item["no_alert_for_skipped_runs"] = emailNotification.NoAlertForSkippedRuns
		result = append(result, item)
	}

	return result
}

func resourceDatabricksJobExpandSchedule(d []interface{}) databricks.CronSchedule {
	m := d[0].(map[string]interface{})

	result := databricks.CronSchedule{}

	if v, ok := getOk(m, "quartz_cron_expression"); ok {
		result.QuartzCronExpression = v.(string)
	}

	if v, ok := getOk(m, "timezone_id"); ok {
		result.TimezoneId = v.(string)
	}

	return result
}

func resourceDatabricksJobFlattenSchedule(schedule *databricks.CronSchedule) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	if schedule != nil {
		item := make(map[string]interface{})
		item["quartz_cron_expression"] = schedule.QuartzCronExpression
		item["timezone_id"] = schedule.TimezoneId
		result = append(result, item)
	}

	return result
}
