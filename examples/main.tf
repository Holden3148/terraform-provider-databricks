resource "databricks_cluster" "example-cluster" {
  num_workers = 1
  cluster_name = "[TF] example cluster"
  spark_version = "4.2.x-scala2.11"
  node_type_id = "r4.xlarge"
  driver_node_type_id = "r4.xlarge"
  ssh_public_keys = ["a", "b"]
  autotermination_minutes = 10
  enable_elastic_disk = false

  aws_attributes {
    ebs_volume_type = "GENERAL_PURPOSE_SSD"
    ebs_volume_count = 1
    ebs_volume_size = 100
    zone_id = "us-west-2c"
    instance_profile_arn = "arn:aws:iam::370025973162:instance-profile/terraform/ec2/tubi-sparknode-production"
  }

  spark_conf {
    spark.databricks.delta.preview.enabled = "true"
  }

  custom_tags {
    creator = "terraform-databricks-provider"
  }

  cluster_log_conf {
    s3 {
      destination = "s3://some-bucket-name"
      region = "us-west-2"
    }
  }

  spark_env_vars {
    a = "b"
  }
}
