resource "databricks_cluster" "example-cluster" {
  name = "[TF] example cluster"
  spark_version = "4.2.x-scala2.11"
  node_type_id = "r3.xlarge"
  num_workers = 1
}
