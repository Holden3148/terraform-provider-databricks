resource "databricks_cluster" "example-cluster" {
  num_workers   = 1
  cluster_name  = "[TF] example cluster"
  spark_version = "4.2.x-scala2.11"

  spark_conf = {
    spark.databricks.delta.preview.enabled = "true"
  }

  aws_attributes = {
    ebs_volume_type      = "GENERAL_PURPOSE_SSD"
    ebs_volume_count     = 1
    ebs_volume_size      = 100
    zone_id              = "us-west-2c"
    instance_profile_arn = "${var.arn}"
  }

  node_type_id        = "r4.xlarge"
  driver_node_type_id = "r4.xlarge"
  ssh_public_keys     = ["a", "b"]

  custom_tags = {
    creator = "terraform-databricks-provider"
  }

  cluster_log_conf = {
    s3 {
      destination = "s3://some-bucket-name"
      region      = "us-west-2"
    }
  }

  spark_env_vars = {
    a = "b"
  }

  autotermination_minutes = 10
  enable_elastic_disk     = false
}

resource "databricks_job" "example-job-from-new-cluster" {
  new_cluster = {
    num_workers = 1

    //    cluster_name = "[TF] example cluster"
    spark_version = "4.2.x-scala2.11"

    spark_conf = {
      spark.databricks.delta.preview.enabled = "true"
    }

    aws_attributes = {
      ebs_volume_type      = "GENERAL_PURPOSE_SSD"
      ebs_volume_count     = 1
      ebs_volume_size      = 100
      zone_id              = "us-west-2c"
      instance_profile_arn = "${var.arn}"
    }

    node_type_id        = "r4.xlarge"
    driver_node_type_id = "r4.xlarge"
    ssh_public_keys     = ["a", "b"]

    custom_tags = {
      creator = "terraform-databricks-provider"
    }

    cluster_log_conf = {
      s3 {
        destination = "s3://some-bucket-name"
        region      = "us-west-2"
      }
    }

    spark_env_vars = {
      a = "b"
    }

    //    autotermination_minutes = 10
    enable_elastic_disk = false
  }

  spark_jar_task = {
    jar_uri         = "some.jar"
    main_class_name = "com.example.Application"
    parameters      = []
  }

  name = "[TF] example job from existing cluster"

  libraries = {
    jar = "dbfs:/FileStore/jars/some.jar"
  }

  libraries = {
    pypi = {
      package = "some-pypi.egg"
      repo    = "com.example"
    }
  }

  email_notifications = {
    on_start                  = []
    on_success                = []
    on_failure                = ["somebody@example.com"]
    no_alert_for_skipped_runs = true
  }

  timeout_seconds           = 3600
  max_retries               = 2
  min_retry_interval_millis = 60000
  retry_on_timeout          = false

  schedule = {
    quartz_cron_expression = "0 15 22 ? * *"
    timezone_id            = "America/Los_Angeles"
  }

  max_concurrent_runs = 1
}

resource "databricks_job" "example-spark-jar-job" {
  existing_cluster_id = "${databricks_cluster.example-cluster.id}"

  spark_jar_task = {
    jar_uri         = "some.jar"
    main_class_name = "com.example.Application"
    parameters      = []
  }

  name = "[TF] example spark jar job"
}

resource "databricks_job" "example-notebook-job-from-existing-cluster" {
  existing_cluster_id = "${databricks_cluster.example-cluster.id}"

  notebook_task = {
    notebook_path = "/some-path"

    // FIXME(Chiyu): databricks return {"error_code":"MALFORMED_REQUEST","message":"Could not parse request object: Expected 'key' and 'value' to be set for JSON map field base_parameters, got '' instead."}
    //    base_parameters = {
    //      a = "b"
    //    }
    //    base_parameters = {
    //      c = "d"
    //    }
  }

  name = "[TF] example notebook job"
}

// FIXME(Chiyu): databricks return {"error_code":"INVALID_PARAMETER_VALUE","message":"Invalid python file URI: dbfs://some-file.py. Please visit Databricks user guide for supported URI schemes."}
//resource "databricks_job" "example-spark-python-job" {
//  existing_cluster_id = "${databricks_cluster.example-cluster.id}"
//
//  spark_python_task = {
//    python_file = "dbfs://some-file.py"
//    parameters  = ["a", "b"]
//  }
//
//  name = "[TF] example spark python job"
//}

resource "databricks_job" "example-spark-submit-job" {
  new_cluster = {
    num_workers = 1
    spark_version = "4.2.x-scala2.11"
    node_type_id        = "r4.xlarge"
    aws_attributes = {
      ebs_volume_type      = "GENERAL_PURPOSE_SSD"
      ebs_volume_count     = 1
      ebs_volume_size      = 100
    }
  }

  spark_submit_task = {
    parameters = ["a", "b"]
  }

  name = "[TF] example spark submit job"
}
