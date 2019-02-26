package databricks

import (
	"github.com/cattail/databricks-sdk-go/databricks"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"log"
	"net/http"
	"time"
)

func resourceDatabricksCluster() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabricksClusterCreate,
		Read:   resourceDatabricksClusterRead,
		Update: resourceDatabricksClusterUpdate,
		Delete: resourceDatabricksClusterDelete,

		Schema: map[string]*schema.Schema{
			"num_workers": {
				Type:     schema.TypeInt,
				Optional: true,
				// FIXME(Chiyu): we can't reuse cluster resource schema in job if we define ConflictsWith here.
				//ConflictsWith: []string{"autoscale"},
			},
			"autoscale": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"min_workers": {
							Type:     schema.TypeInt,
							Required: true,
						},
						"max_workers": {
							Type:     schema.TypeInt,
							Required: true,
						},
					},
				},
				//ConflictsWith: []string{"num_workers"},
			},
			"cluster_name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"spark_version": {
				Type:     schema.TypeString,
				Required: true,
			},
			"spark_conf": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"aws_attributes": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"zone_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"instance_profile_arn": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"ebs_volume_type": {
							Type:     schema.TypeString,
							Optional: true,
							ValidateFunc: validation.StringInSlice([]string{
								string(databricks.GENERAL_PURPOSE_SSD_ClustersEbsVolumeType),
								string(databricks.THROUGHPUT_OPTIMIZED_HDD_ClustersEbsVolumeType),
							}, true),
						},
						"ebs_volume_count": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"ebs_volume_size": {
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},
			"node_type_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"driver_node_type_id": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"ssh_public_keys": {
				Type:     schema.TypeList,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"custom_tags": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"cluster_log_conf": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"dbfs": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"destination": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
						"s3": {
							Type:     schema.TypeList,
							Optional: true,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"destination": {
										Type:     schema.TypeString,
										Required: true,
									},
									"region": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"endpoint": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
					},
				},
			},
			"spark_env_vars": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"autotermination_minutes": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"enable_elastic_disk": {
				Type:     schema.TypeBool,
				Optional: true,
			},
		},
	}
}

func resourceDatabricksClusterCreate(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).ClusterApi

	request := getClusterSettings(d)
	logJSON("[DEBUG] Creating cluster", request)

	resp, _, err := client.CreateCluster(nil, request)
	if err != nil {
		return err
	}

	d.SetId(resp.ClusterId)

	return resourceDatabricksClusterRead(d, m)
}

func resourceDatabricksClusterUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).ClusterApi

	clusterId := d.Id()

	waitClusterState(client, clusterId, []databricks.ClustersClusterState{
		databricks.RUNNING_ClustersClusterState,
		databricks.TERMINATED_ClustersClusterState,
	})

	request := getClusterSettings(d)
	request.ClusterId = clusterId
	logJSON("[DEBUG] Updating cluster", request)

	_, err := client.EditCluster(nil, request)
	if err != nil {
		return err
	}

	return resourceDatabricksClusterRead(d, m)
}

func resourceDatabricksClusterDelete(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).ClusterApi

	log.Printf("[DEBUG] Deleting cluster: %s", d.Id())

	_, err := client.PermanentDeleteCluster(nil, databricks.ClustersPermanentDeleteRequest{
		ClusterId: d.Id(),
	})
	if err != nil {
		return err
	}

	d.SetId("")

	return nil
}

func resourceDatabricksClusterRead(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).ClusterApi

	resp, httpResponse, err := client.GetCluster(nil, d.Id())
	if err != nil {
		if resourceDatabricksClusterNotExistsError(httpResponse) {
			log.Printf("[WARN] Cluster (%s) not found, removing from state", d.Id())
			d.SetId("")
			return nil
		}
		return err
	}

	clusterSettings, err := convertClusterInfoToSettings(resp)
	if err != nil {
		return err
	}

	return setClusterSettings(d, *clusterSettings)
}

func resourceDatabricksClusterNotExistsError(httpResponse *http.Response) bool {
	return httpResponse.StatusCode >= 400
}

func resourceDatabricksClusterExpandAutoscale(autoscale []interface{}) databricks.ClustersAutoScale {
	m := autoscale[0].(map[string]interface{})

	return databricks.ClustersAutoScale{
		MinWorkers: int32(m["min_workers"].(int)),
		MaxWorkers: int32(m["max_workers"].(int)),
	}
}

func resourceDatabricksClusterFlattenAutoscale(autoscale *databricks.ClustersAutoScale) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	if autoscale != nil {
		result = append(result, map[string]interface{}{
			"min_workers": autoscale.MinWorkers,
			"max_workers": autoscale.MaxWorkers,
		})
	}
	return result
}

func resourceDatabricksClusterExpandAwsAttributes(awsAttributes []interface{}) databricks.ClustersAwsAttributes {
	m := awsAttributes[0].(map[string]interface{})

	result := databricks.ClustersAwsAttributes{}

	if v, ok := getOk(m, "zone_id"); ok {
		result.ZoneId = v.(string)
	}

	if v, ok := getOk(m, "instance_profile_arn"); ok {
		result.InstanceProfileArn = v.(string)
	}

	if v, ok := getOk(m, "ebs_volume_type"); ok {
		volumeType := databricks.ClustersEbsVolumeType(v.(string))
		result.EbsVolumeType = &volumeType
	}

	if v, ok := getOk(m, "ebs_volume_count"); ok {
		result.EbsVolumeCount = int32(v.(int))
	}

	if v, ok := getOk(m, "ebs_volume_size"); ok {
		result.EbsVolumeSize = int32(v.(int))
	}

	return result
}

func resourceDatabricksClusterFlattenAwsAttributes(awsAttributes *databricks.ClustersAwsAttributes) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	if awsAttributes != nil {
		attrs := make(map[string]interface{})
		attrs["zone_id"] = awsAttributes.ZoneId
		attrs["instance_profile_arn"] = awsAttributes.InstanceProfileArn
		if awsAttributes.EbsVolumeType != nil {
			attrs["ebs_volume_type"] = string(*awsAttributes.EbsVolumeType)
			attrs["ebs_volume_count"] = int(awsAttributes.EbsVolumeCount)
			attrs["ebs_volume_size"] = int(awsAttributes.EbsVolumeSize)
		}

		result = append(result, attrs)
	}

	return result
}

func resourceDatabricksClusterExpandClusterLogConf(clusterLogConf []interface{}) databricks.ClustersClusterLogConf {
	m := clusterLogConf[0].(map[string]interface{})

	result := databricks.ClustersClusterLogConf{}

	if v, ok := getOk(m, "dbfs"); ok {
		clustersClusterLogConfDbfs := databricks.ClustersClusterLogConfDbfs{}
		clustersClusterLogConfDbfsElem := v.([]interface{})[0].(map[string]interface{})
		if v, ok := clustersClusterLogConfDbfsElem["destination"]; ok {
			clustersClusterLogConfDbfs.Destination = v.(string)
		}
		result.Dbfs = &clustersClusterLogConfDbfs
	}

	if v, ok := getOk(m, "s3"); ok {
		clustersClusterLogConfS3 := databricks.ClustersClusterLogConfS3{}
		clustersClusterLogConfS3Elem := v.([]interface{})[0].(map[string]interface{})
		if v, ok := clustersClusterLogConfS3Elem["destination"]; ok {
			clustersClusterLogConfS3.Destination = v.(string)
		}
		if v, ok := clustersClusterLogConfS3Elem["region"]; ok {
			clustersClusterLogConfS3.Region = v.(string)
		}
		if v, ok := clustersClusterLogConfS3Elem["endpoint"]; ok {
			clustersClusterLogConfS3.Endpoint = v.(string)
		}
		result.S3 = &clustersClusterLogConfS3
	}

	return result
}

func resourceDatabricksClusterFlattenClusterLogConf(clusterLogConf *databricks.ClustersClusterLogConf) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	if clusterLogConf != nil {
		attrs := make(map[string]interface{})

		if clusterLogConf.Dbfs != nil {
			innerAttrs := make(map[string]interface{})
			innerAttrs["destination"] = clusterLogConf.Dbfs.Destination
			attrs["dbfs"] = []interface{}{innerAttrs}
		}

		if clusterLogConf.S3 != nil {
			innerAttrs := make(map[string]interface{})
			innerAttrs["destination"] = clusterLogConf.S3.Destination
			innerAttrs["region"] = clusterLogConf.S3.Region
			innerAttrs["endpoint"] = clusterLogConf.S3.Endpoint
			attrs["s3"] = []interface{}{innerAttrs}
		}

		result = append(result, attrs)
	}

	return result
}

func waitClusterState(client *databricks.ClusterApiService, clusterId string, states []databricks.ClustersClusterState) {
	res, _, _ := client.GetCluster(nil, clusterId)

	newStates := make([]interface{}, len(states))
	for i := range states {
		newStates[i] = states[i]
	}

	for !find(newStates, *res.State) {
		res, _, _ = client.GetCluster(nil, clusterId)
		time.Sleep(5 * time.Second)
		log.Printf("[DEBUG] Waiting cluster enter %s state from %s\n", states, *res.State)
	}
}

func getClusterSettings(d interface{}) databricks.NewCluster {
	clusterSettings := databricks.NewCluster{
		SparkVersion: get(d, "spark_version").(string),
		NodeTypeId:   get(d, "node_type_id").(string),
	}

	if v, ok := getOk(d, "num_workers"); ok {
		clusterSettings.NumWorkers = int32(v.(int))
	}

	if v, ok := getOk(d, "autoscale"); ok {
		autoscale := resourceDatabricksClusterExpandAutoscale(v.([]interface{}))
		clusterSettings.Autoscale = &autoscale
	}

	if v, ok := getOk(d, "cluster_name"); ok {
		clusterSettings.ClusterName = v.(string)
	}

	if v, ok := getOk(d, "spark_conf"); ok {
		clusterSettings.SparkConf = toMapString(v)
	}

	if v, ok := getOk(d, "aws_attributes"); ok {
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(v.([]interface{}))
		clusterSettings.AwsAttributes = &awsAttributes
	}

	if v, ok := getOk(d, "driver_node_type_id"); ok {
		clusterSettings.DriverNodeTypeId = v.(string)
	}

	if v, ok := getOk(d, "ssh_public_keys"); ok {
		clusterSettings.SshPublicKeys = toSliceString(v)
	}

	if v, ok := getOk(d, "custom_tags"); ok {
		clusterSettings.CustomTags = toMapString(v)
	}

	if v, ok := getOk(d, "cluster_log_conf"); ok {
		clusterLogConf := resourceDatabricksClusterExpandClusterLogConf(v.([]interface{}))
		clusterSettings.ClusterLogConf = &clusterLogConf
	}

	if v, ok := getOk(d, "spark_env_vars"); ok {
		clusterSettings.SparkEnvVars = toMapString(v)
	}

	if v, ok := getOk(d, "autotermination_minutes"); ok {
		clusterSettings.AutoterminationMinutes = int32(v.(int))
	}

	if v, ok := getOk(d, "enable_elastic_disk"); ok {
		clusterSettings.EnableElasticDisk = v.(bool)
	}

	return clusterSettings
}

func setClusterSettings(d interface{}, clusterSettings databricks.NewCluster) error {
	err := set(d, "spark_version", clusterSettings.SparkVersion)
	if err != nil {
		return err
	}

	err = set(d, "node_type_id", clusterSettings.NodeTypeId)
	if err != nil {
		return err
	}

	err = set(d, "num_workers", clusterSettings.NumWorkers)
	if err != nil {
		return err
	}

	err = set(d, "autoscale", resourceDatabricksClusterFlattenAutoscale(clusterSettings.Autoscale))
	if err != nil {
		return err
	}

	err = set(d, "cluster_name", clusterSettings.ClusterName)
	if err != nil {
		return err
	}

	err = set(d, "spark_conf", clusterSettings.SparkConf)
	if err != nil {
		return err
	}

	err = set(d, "aws_attributes", resourceDatabricksClusterFlattenAwsAttributes(clusterSettings.AwsAttributes))
	if err != nil {
		return err
	}

	err = set(d, "driver_node_type_id", clusterSettings.DriverNodeTypeId)
	if err != nil {
		return err
	}

	err = set(d, "ssh_public_keys", clusterSettings.SshPublicKeys)
	if err != nil {
		return err
	}

	err = set(d, "custom_tags", clusterSettings.CustomTags)
	if err != nil {
		return err
	}

	err = set(d, "cluster_log_conf", resourceDatabricksClusterFlattenClusterLogConf(clusterSettings.ClusterLogConf))
	if err != nil {
		return err
	}

	err = set(d, "spark_env_vars", clusterSettings.SparkEnvVars)
	if err != nil {
		return err
	}

	err = set(d, "autotermination_minutes", clusterSettings.AutoterminationMinutes)
	if err != nil {
		return err
	}

	err = set(d, "enable_elastic_disk", clusterSettings.EnableElasticDisk)
	if err != nil {
		return err
	}

	return nil
}
