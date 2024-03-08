provider "aws" {
    region = "us-east-1"
    profile = "default"

}

resource "aws_emr_cluster" "land_registry_project" {
    name = "emr-land-registry"
    release_label = "emr-6.3.0"
    applications = ["Spark", 
                    "Hadoop", 
                    "JupyterEnterpriseGateway",
                    "JupyterHub"]
    
    ec2_attributes {
    subnet_id                         = "subnet-00e08835be6be7db3"
    emr_managed_master_security_group = "sg-0c555bdbf93d0700f"
    emr_managed_slave_security_group  = "sg-0a3218a534484cb5a"
    instance_profile                  = "arn:aws:iam::008768223997:instance-profile/AmazonEMR-InstanceProfile-20240201T172733"
    }

    master_instance_group {
    instance_type = "m4.large"
    }

    core_instance_group {
        instance_type = "c4.large"
        instance_count = 1
        ebs_config {
            size = "20"
            type = "gp3"
            volumes_per_instance = 1
        }

        
    }

    service_role = "arn:aws:iam::008768223997:role/service-role/AmazonEMR-ServiceRole-20240201T172749"
}


