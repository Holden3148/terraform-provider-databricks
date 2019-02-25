package databricks

func find(source []interface{}, predict interface{}) bool {
	for _, v := range source {
		if v == predict {
			return true
		}
	}
	return false
}
