package databricks

import (
	"encoding/json"
	"github.com/cattail/databricks-sdk-go/databricks"
	"github.com/hashicorp/terraform/helper/schema"
	"log"
	"reflect"
)

func find(source []interface{}, predict interface{}) bool {
	for _, v := range source {
		if v == predict {
			return true
		}
	}
	return false
}

func toMapString(d interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range d.(map[string]interface{}) {
		result[k] = v.(string)
	}
	return result
}

func toSliceMapString(d interface{}) []map[string]string {
	c := d.([]interface{})
	result := make([]map[string]string, len(c))
	for i, v := range c {
		result[i] = toMapString(v)
	}
	return result
}

func toSliceString(d interface{}) []string {
	c := d.([]interface{})
	result := make([]string, len(c))
	for i, v := range c {
		result[i] = v.(string)
	}
	return result
}

func get(d interface{}, key string) interface{} {
	switch d.(type) {
	case *schema.ResourceData:
		v := d.(*schema.ResourceData)
		return v.Get(key)
	default:
		v := d.(map[string]interface{})[key]
		return v
	}
}

// return ok as true when d is not nil or empty slice
func getOk(d interface{}, key string) (interface{}, bool) {
	switch d.(type) {
	case *schema.ResourceData:
		v := d.(*schema.ResourceData)
		return v.GetOk(key)

	default:
		v := d.(map[string]interface{})[key]

		if v == nil {
			return nil, false
		}

		if reflect.TypeOf(v).Kind() == reflect.Slice {
			return v, len(v.([]interface{})) != 0
		}

		return v, true
	}
}

func set(d interface{}, key string, value interface{}) error {
	switch d.(type) {
	case *schema.ResourceData:
		v := d.(*schema.ResourceData)
		return v.Set(key, value)
	default:
		v := d.(map[string]interface{})
		v[key] = value
		return nil
	}
}

func logJSON(message string, d interface{}) {
	str, err := json.Marshal(d)
	if err != nil {
		log.Printf("%s: %s\n", message, d)
		return
	}

	log.Printf("%s: %s\n", message, str)
}

// hack to convert struct NewCluster to struct ClusterInfo
func convertClusterInfoToSettings(clusterInfo databricks.ClustersClusterInfo) (*databricks.NewCluster, error) {
	clusterSettings := databricks.NewCluster{}

	bytes, err := json.Marshal(clusterInfo)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &clusterSettings)
	if err != nil {
		return nil, err
	}

	return &clusterSettings, nil
}
