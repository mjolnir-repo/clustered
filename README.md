# Automated Spark Cluster using AWS EC2 instances
This project fucuses on launching a Spark Cluster consisting one Master node and multiple(as declared in config file) Slaves nodes. The utility has following features:
1. Create One Image contaning all required configurations automatically
2. Launch all required AWS services using Cloudformation Stack including EC2 instances to be used as nodes
3. Configure the EC2 instances to act as Spark Nodes
4. Launch Spark services (master and slave)
6. Add New Nodes
7. Stop all Spark servces
8. Clean up all AWS services by deleting Cloudformation Stack
