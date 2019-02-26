package databricks

import (
	"encoding/json"
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

// return ok as true when d is not nil or empty slice
func getOK(d map[string]interface{}, key string) (interface{}, bool) {
	v := d[key]

	if v == nil {
		return nil, false
	}

	if reflect.TypeOf(v).Kind() == reflect.Slice {
		return v, len(v.([]interface{})) != 0
	}

	return v, true
}

func logJSON(message string, d interface{}) {
	str, err := json.Marshal(d)
	if err != nil {
		log.Printf("%s: %s\n", message, d)
		return
	}

	log.Printf("%s: %s\n", message, str)
}
