# Automated Spark Cluster using AWS EC2 instances
#### This project fucuses on launching a Spark Cluster consisting one Master node and multiple(as declared in config file) Slaves nodes. The utility has following features:
1. Launch one EC2 instance from AWS Management Console. Download spark, configure necessary properties in the EC2 instance. Create one AMI from the EC2 and store it. (One time Activity)
2. Create one network to host the cluster using VPC, Subnets, Route Table etc.
3. Launch Master and Slave nodes using AWS EC2. These instances are launched using one AMI, which is cretaed in Step one.
4. Configure the Nodes to create the cluster.
5. Start the Spark service.
6. Stop the Spark service.
7. Terminate the Nodes
8. Delete network related services.

#### N.B.: Please note, in this version following features are not provided, those will come in future versions:
* Add worker node to running cluster.
* Whitelist IP in a running cluster.
* Currently the AMI is predefined manually(although one time activity), In future releases, this step will be automated.
* One python package version of the utility will be provided to integrate it with other applications.

#### To support all these features total nine Notebooks are available. Each Notebook serves a specific purpose. These Notebooks are to executed in proper sequence to create the Cluster. The Notebooks are:
1. cloudformation_stack_creation.ipynb
2. master_node_creation.ipynb
3. slave_nodes_creation.ipynb
4. spark_cluster_configure.ipynb
5. spark_cluster_start.ipynb
6. spark_cluster_stop.ipynb
7. slave_node_termination.ipynb
8. master_node_termination.ipynb
9. cloudformation_stack_deletion.ipynb

To run this Utility, one configuration file must be provided. Some configurations are mandatory and some are optional. Mandatory configurations are provided in __Bold__. The utility expects the configuration file ('cluster_config.json') to be present in the code base directory itself.

#### Following are the properties to be configured:
* Region: AWS Region. Default value is __us-east-1__.
* AZList: AWS Availability Zones. Default values is __[us-east-1a, us-east-1b]__. Please provide valid Availability Zone values if configuring explicitly.
* __CidrBlock__: CIDR Block for the VPC to be launched. This is a __mandatory__ field. Please note, irrespective of the provided value, masking will be '__/22__'
* __KeyPairPath__: Key pair file (.pem) path. This is also __mandatory__ field.
* __KeyPairName__: Key pair file (.pem) name. Please __DO NOT__ provide the extension. __Only the name__ is required. This is also __mandatory__ field.
* ProjectTag: Project name. Default value is '__SparkCluster__'.
* IPWhitelist: List of IPs to be whitelisted.
* UserName: Utility user name, to distinguise separate verisons of execution. Default value is '__root__'. Please note __only one cluster__ is allowed per user.
* InstanceType: EC2 instance type for the nodes. Default value is '__t2.micro__'.
* __WorkspaceDirectory__: Local directory that will be used as workspace by the uitlity durinf run time. This is a __mandatory__ field.
* SlaveCount: Number of Slave nodes. Default value is __3__.