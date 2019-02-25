package databricks

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

func toSliceString(d interface{}) []string {
	c := d.([]interface{})
	result := make([]string, len(c))
	for i, v := range c {
		result[i] = v.(string)
	}
	return result
}
