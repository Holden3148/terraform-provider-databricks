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

	log.Print("[DEBUG] Creating cluster")

	request := databricks.ClustersCreateRequest{
		SparkVersion: d.Get("spark_version").(string),
		NodeTypeId:   d.Get("node_type_id").(string),
	}

	if v, ok := d.GetOk("num_workers"); ok {
		request.NumWorkers = int32(v.(int))
	}

	if v, ok := d.GetOk("autoscale"); ok {
		autoscale := resourceDatabricksClusterExpandAutoscale(v.([]interface{}))
		request.Autoscale = &autoscale
	}

	if v, ok := d.GetOk("cluster_name"); ok {
		request.ClusterName = v.(string)
	}

	if v, ok := d.GetOk("spark_conf"); ok {
		request.SparkConf = toMapString(v)
	}

	if v, ok := d.GetOk("aws_attributes"); ok {
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(v.([]interface{}))
		request.AwsAttributes = &awsAttributes
	}

	if v, ok := d.GetOk("driver_node_type_id"); ok {
		request.DriverNodeTypeId = v.(string)
	}

	if v, ok := d.GetOk("ssh_public_keys"); ok {
		request.SshPublicKeys = toSliceString(v)
	}

	if v, ok := d.GetOk("custom_tags"); ok {
		request.CustomTags = toMapString(v)
	}

	if v, ok := d.GetOk("cluster_log_conf"); ok {
		clusterLogConf := resourceDatabricksClusterExpandClusterLogConf(v.([]interface{}))
		request.ClusterLogConf = &clusterLogConf
	}

	if v, ok := d.GetOk("spark_env_vars"); ok {
		request.SparkEnvVars = toMapString(v)
	}

	if v, ok := d.GetOk("autotermination_minutes"); ok {
		request.AutoterminationMinutes = int32(v.(int))
	}

	if v, ok := d.GetOk("enable_elastic_disk"); ok {
		request.EnableElasticDisk = v.(bool)
	}

	resp, _, err := client.CreateCluster(nil, request)
	if err != nil {
		return err
	}

	d.SetId(resp.ClusterId)

	log.Printf("[DEBUG] Cluster ID: %s", d.Id())

	return resourceDatabricksClusterRead(d, m)
}

func resourceDatabricksClusterUpdate(d *schema.ResourceData, m interface{}) error {
	client := m.(*databricks.APIClient).ClusterApi

	log.Printf("[DEBUG] Updating cluster: %s", d.Id())

	clusterId := d.Id()

	waitClusterState(client, clusterId, []databricks.ClustersClusterState{
		databricks.RUNNING_ClustersClusterState,
		databricks.TERMINATED_ClustersClusterState,
	})

	request := databricks.ClustersEditRequest{
		ClusterId:    clusterId,
		SparkVersion: d.Get("spark_version").(string),
		NodeTypeId:   d.Get("node_type_id").(string),
	}

	if v, ok := d.GetOk("num_workers"); ok {
		request.NumWorkers = int32(v.(int))
	}

	if v, ok := d.GetOk("autoscale"); ok {
		autoscale := resourceDatabricksClusterExpandAutoscale(v.([]interface{}))
		request.Autoscale = &autoscale
	}

	if v, ok := d.GetOk("cluster_name"); ok {
		request.ClusterName = v.(string)
	}

	if v, ok := d.GetOk("spark_conf"); ok {
		request.SparkConf = toMapString(v)
	}

	if v, ok := d.GetOk("aws_attributes"); ok {
		awsAttributes := resourceDatabricksClusterExpandAwsAttributes(v.([]interface{}))
		request.AwsAttributes = &awsAttributes
	}

	if v, ok := d.GetOk("driver_node_type_id"); ok {
		request.DriverNodeTypeId = v.(string)
	}

	if v, ok := d.GetOk("ssh_public_keys"); ok {
		request.SshPublicKeys = toSliceString(v)
	}

	if v, ok := d.GetOk("custom_tags"); ok {
		request.CustomTags = toMapString(v)
	}

	if v, ok := d.GetOk("cluster_log_conf"); ok {
		clusterLogConf := resourceDatabricksClusterExpandClusterLogConf(v.([]interface{}))
		request.ClusterLogConf = &clusterLogConf
	}

	if v, ok := d.GetOk("spark_env_vars"); ok {
		request.SparkEnvVars = toMapString(v)
	}

	if v, ok := d.GetOk("autotermination_minutes"); ok {
		request.AutoterminationMinutes = int32(v.(int))
	}

	if v, ok := d.GetOk("enable_elastic_disk"); ok {
		request.EnableElasticDisk = v.(bool)
	}

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

	err = d.Set("spark_version", resp.SparkVersion)
	if err != nil {
		return err
	}

	err = d.Set("node_type_id", resp.NodeTypeId)
	if err != nil {
		return err
	}

	err = d.Set("num_workers", resp.NumWorkers)
	if err != nil {
		return err
	}

	err = d.Set("autoscale", resourceDatabricksClusterFlattenAutoscale(resp.Autoscale))
	if err != nil {
		return err
	}

	err = d.Set("cluster_name", resp.ClusterName)
	if err != nil {
		return err
	}

	err = d.Set("spark_conf", resp.SparkConf)
	if err != nil {
		return err
	}

	err = d.Set("aws_attributes", resourceDatabricksClusterFlattenAwsAttributes(resp.AwsAttributes))
	if err != nil {
		return err
	}

	err = d.Set("driver_node_type_id", resp.DriverNodeTypeId)
	if err != nil {
		return err
	}

	err = d.Set("ssh_public_keys", resp.SshPublicKeys)
	if err != nil {
		return err
	}

	err = d.Set("custom_tags", resp.CustomTags)
	if err != nil {
		return err
	}

	err = d.Set("cluster_log_conf", resourceDatabricksClusterFlattenClusterLogConf(resp.ClusterLogConf))
	if err != nil {
		return err
	}

	err = d.Set("spark_env_vars", resp.SparkEnvVars)
	if err != nil {
		return err
	}

	err = d.Set("autotermination_minutes", resp.AutoterminationMinutes)
	if err != nil {
		return err
	}

	err = d.Set("enable_elastic_disk", resp.EnableElasticDisk)
	if err != nil {
		return err
	}

	return nil
}

func resourceDatabricksClusterNotExistsError(httpResponse *http.Response) bool {
	return httpResponse.StatusCode >= 400
}

func resourceDatabricksClusterExpandAutoscale(autoscale []interface{}) databricks.ClustersAutoScale {
	autoscaleElem := autoscale[0].(map[string]interface{})

	return databricks.ClustersAutoScale{
		MinWorkers: int32(autoscaleElem["min_workers"].(int)),
		MaxWorkers: int32(autoscaleElem["max_workers"].(int)),
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
	awsAttributesElem := awsAttributes[0].(map[string]interface{})

	result := databricks.ClustersAwsAttributes{}

	if v, ok := awsAttributesElem["zone_id"]; ok {
		result.ZoneId = v.(string)
	}

	if v, ok := awsAttributesElem["instance_profile_arn"]; ok {
		result.InstanceProfileArn = v.(string)
	}

	if v, ok := awsAttributesElem["ebs_volume_type"]; ok {
		volumeType := databricks.ClustersEbsVolumeType(v.(string))
		result.EbsVolumeType = &volumeType
	}

	if v, ok := awsAttributesElem["ebs_volume_count"]; ok {
		result.EbsVolumeCount = int32(v.(int))
	}

	if v, ok := awsAttributesElem["ebs_volume_size"]; ok {
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
	clusterLogConfElem := clusterLogConf[0].(map[string]interface{})

	result := databricks.ClustersClusterLogConf{}

	if v, ok := clusterLogConfElem["dbfs"]; ok && len(v.([]interface{})) > 0 {
		clustersClusterLogConfDbfs := databricks.ClustersClusterLogConfDbfs{}
		clustersClusterLogConfDbfsElem := v.([]interface{})[0].(map[string]interface{})
		if v, ok := clustersClusterLogConfDbfsElem["destination"]; ok {
			clustersClusterLogConfDbfs.Destination = v.(string)
		}
		result.Dbfs = &clustersClusterLogConfDbfs
	}

	if v, ok := clusterLogConfElem["s3"]; ok && len(v.([]interface{})) > 0 {
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
