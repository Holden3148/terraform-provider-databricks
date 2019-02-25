resource "databricks_cluster" "example-cluster" {
  spark_version = "4.2.x-scala2.11"
  node_type_id = "r3.xlarge"
}
