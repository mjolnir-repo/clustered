# SparkClusterOnAWSEC2
### An automated Spark Cluster on AWS EC2 instances.

## 1. Introduction:
First thing first, this utility is not to be used as a permanent business solution to create and manage a Spark cluster in any production (nor DEV in industry in that matter). Then the obvious question is why am I wasting your time?
To answer that, let me ask a simpler question, Do you ever feel like, __"*it would be great if you had an Spark cluster that can be turned on and off based on requirement to run your late night spark POC codes*"__? If Yes, then you are in the right place, or else you can happily skip the entire article and I promise you will not miss out on much in your life.

Now having the biggest confusion (__Is this another tutorial? - NOOOOOOO__), let's see how this can help your craving of having a small(big is also possible) and simple(complicated is also an option if you are ready to do a bit more) cluster.

In a nut-shell __SparkClusterOnAWSEC2__ is exactly what the name suggests, you can create a cluster within minutes on AWS, run your code, do your work (the time consumed here is totally depends on how optimized your code is, can't help in that buddy!), and stop the cluster and clean up all services that has been on AWS __with a few clicks__. I can not stress enough on the bold part of the sentence. How to do(use) it? Let's dive in...


#### This project focuses on launching a Spark Cluster consisting one Master node and multiple(as declared in config file) Slaves nodes. The utility has following features:
1. Launch one EC2 instance from AWS Management Console. Download spark, configure necessary properties in the EC2 instance. Create one AMI from the EC2 and store it. (One time Activity)
2. Create configuration file.
3. Create one network to host the cluster using VPC, Subnets, Route Table etc.
4. Launch Master and Slave nodes using AWS EC2. These instances are launched using one AMI, which is cretaed in Step one.
5. Configure the Nodes to create the cluster.
6. Start the Spark service.
7. Stop the Spark service.
8. Terminate the Nodes
9. Delete network related services.

We will go through each step one by one and give you enough assistance to use the utility in section 3.

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


## 2. Pre-Configuration:
To use SparkClusterOnAWSEC2 utility following points must be checked. These are one time tasks, once you have done it, next time onwards you need not do these.
1. Create AWS account, not required if already have one (https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account).
2. Create one IAM user having full access to EC2, Cloudformation, VPC, S3 (https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html).
3. Install AWS CLI (https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
3. Configure the IAM user in AWS CLI.(https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
5. Have One AMI with all required software(spark, python, ssh) installed in it (Refer to AMI creation step 3.1).
6. Make sure SSH is available or install the same.


## 3. Usage:

Okay, boring part is over. If you are still with me that means you really need a cluster of you rown. No worries, no worries at all. Now we can start using the utility. From here onwards we will go step-by-step.
### 3.1. AMI Creation:

In this step we will create one AMI(Amazon Machine Image) that will be used as base image for each node. If you wish to install any other softwares in each node or want to do any configurations, I suggest you do those in this image. Here we will do follwoing steps:
1. Launch an EC2 instance using Amazon Linux 2 AMI(https://docs.aws.amazon.com/quickstarts/latest/vmlaunch/step-1-launch-instance.html). Make sure the instance is launched with an public DNS.
2. Login into the EC2 instance using the public DNS (https://docs.aws.amazon.com/quickstarts/latest/vmlaunch/step-2-connect-to-instance.html).
2. Run following commands:
    - #### Update all existing libraries:
        - _sudo yum update -y_

    - #### Install Java 8:
        - _sudo yum install java-1.8.0-openjdk-devel_
        - _java -version_ (In case of unsatisfactory results, try to install java using some other method).

    - #### Download and Install Scala:
        - _wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm_
        - _sudo yum install scala-2.11.8.rpm_
        - _scala -version_ (In case of unsatisfactory results, try to install java using some other method).

    - #### Download and extract Spark:
        - _wget http://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz_
        - _sudo tar -zxvf spark-2.4.5-bin-hadoop2.7.tgz_

    __Please note__, to use PySpark, Install Python 3.7, pip3, py4j and findspark after these steps.
3. Create an AMI from this EC2(https://docs.aws.amazon.com/toolkit-for-visual-studio/latest/user-guide/tkv-create-ami-from-instance.html). Please use proper ProjectName tag (provide the same ProjectName in configuration file in next section), or else it will not be visible to utility.
4. Terminate the EC2 instance.

__*N.B.* This is a one time activity. Once an AMI is created. It can be re-used for each run.__
### 3.2. Cluster Configuration:

To run this Utility, one configuration file must be provided. Some configurations are mandatory and some are optional. Mandatory configurations are provided in __Bold__. The utility expects the configuration file ('cluster_config.json') to be present in the code base directory itself.
#### Following are the properties to be configured:
* Region: AWS Region. Default value is __us-east-1__.
* AZList: AWS Availability Zones. Default values is __[us-east-1a, us-east-1b]__. Please provide valid Availability Zone values if configuring explicitly.
* __CidrBlock__: CIDR Block for the VPC to be launched. This is a __mandatory__ field. Please note, irrespective of the provided value, masking will be '__/22__'
* __KeyPairPath__: Key pair file (.pem) path. This is also __mandatory__ field.
* __KeyPairName__: Key pair file (.pem) name. Please __DO NOT__ provide the extension. __Only the name__ is required. This is also __mandatory__ field.
* ProjectTag: Project name. Default value is '__SparkCluster__'.
* __IPWhitelist__: List of IPs to be whitelisted. Any machine that is supposed to behave as a Edge node, needs to be whitelisted. If you want to whitelist entire internet(please don't do that, major security concern) provide __[0.0.0.0/0]__.
* UserName: Utility user name, to distinguise separate verisons of execution. Default value is '__root__'. Please note __only one cluster__ is allowed per user.
* InstanceType: EC2 instance type for the nodes. Default value is '__t2.micro__'.
* __WorkspaceDirectory__: Local directory that will be used as workspace by the uitlity durinf run time. This is a __mandatory__ field.
* SlaveCount: Number of Slave nodes. Default value is __3__.

### 3.3. Create Network:
From here onwards we will execute provided Notebooks to Complete a step. Execute following Notebook:
* __*cloudformation_stack_creation.ipynb*__

### 3.4. Launch the Nodes (Master and Slave):
Execute following Notebook in provided sequence:
1. __*master_node_creation.ipynb*__
2. __*slave_nodes_creation.ipynb*__

### 3.5. Configure the Nodes:
Execute following Notebook:
* __*spark_cluster_configure.ipynb*__

### 3.6. Start the Cluster:
Execute following Notebook:
* __*spark_cluster_start.ipynb*__

### 3.7. Stop the Cluster:
Execute following Notebook:
* __*spark_cluster_stop.ipynb*__

### 3.8. Terminate the Nodes:
Execute following Notebooks in provided sequence:
1. __*slave_node_termination.ipynb*__
2. __*master_node_termination.ipynb*__

### 3.9. Clean up other AWS services:
Execute following Notebook:
* __*cloudformation_stack_deletion.ipynb*__

## 4. Next Version Trailer:
We are planning to include following features(feels really important now that we talk about these) in next version. Stay tuned.
* Add worker node to running cluster.
* Whitelist IP in a running cluster, without stoping the cluster.
* Currently the AMI is predefined manually(although one time activity), In future releases, this step will be automated.
* One python package version of the utility will be provided to integrate it with other applications.

## Conclusion:
Let me end the article with thanking you for going through the article. It will be highly appreciated if anyone have any suggestions, ideas to implement. In case of any queries, suggestion, I am available on _+91-9593090126_ and saumalya75@gmail.com. The entire project is available on my BitBucket repository - https://bitbucket.org/saumalya75/sparkclusteronaws/src/master. The repository can be cloned from 'git@bitbucket.org:saumalya75/sparkclusteronaws.git'.