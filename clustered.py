#!/usr/bin/env python

import click
import pprint
from clustered.env import env
from clustered.engine.repository_engine import RepositoryEngine
from clustered.database import db_obj
from clustered.models import Base


"""
This is a command line tool to execute different functionalities of SparkClusterOnAWSEC2 utility. Using this tool user can easility create, run, stop, destroy Spark Clusters as per requirement.

Pre-Requisite:
	1. AWS account,
	2. Install awacli,
	3. AWS Access key, Secret Key and Region needs to be configured in environment using proper variable,
	4. Have one AMI in AWS account that has Spark configured.

Please go through the README.md for more detailed information.

Happy Clustering!!!
"""

@click.group()
def clustered_cli():
	pass

@click.command('setup-db')
def setup_database():
	force = click.prompt("DEBUG:Do you really want to setup empty database(Y/N)?", type=str)
	if force.lower() == 'y':
		Base.metadata.create_all(db_obj.db_engine)
		click.echo("INFO: Database set up completed.")
	else:
		click.echo("INFO: Database set up cancelled.")

@click.command('create-repo')
@click.argument('repo_name', type=str, nargs=1)
@click.option('-a', '--aws-access-key', 'aws_access_key', default='', help="Optional access key of AWS account, if not provided, env value will be used.")
@click.option('-r', '--aws-region', 'aws_region', default='', help="Optional region of AWS account, if not provided, env value will be used.")
def create_repository(repo_name, **kwargs):
	if kwargs.get('aws_access_key', ''):
		aws_access_key = kwargs['aws_access_key']
		aws_secret_key = click.prompt("AWS Secret Key", hide_input=True)
	else:
		aws_access_key = ''
		aws_secret_key = ''

	if kwargs.get('aws_region', ''):
		aws_region = kwargs['aws_region']
	else:
		aws_region = ''

	response = RepositoryEngine.create_repository(repo_name, aws_access_key, aws_secret_key, aws_region)
	if response:
		click.echo("INFO: '" + repo_name + "' is created successfully.")
	else:
		click.echo("ERROR: '" + repo_name + "' is not created successfully.")

@click.command('ls-repos')
def get_repository_list():
	resp = RepositoryEngine.list_repositories()
	print(resp)

@click.command('delete-repo')
@click.argument('repo_name', type=str, nargs=1)
def delete_repository(repo_name):
	response = RepositoryEngine.delete_repository(repo_name)
	if response:
		click.echo("INFO: '" + repo_name + "' is deleted successfully.")
	else:
		click.echo("ERROR: '" + repo_name + "' is not deleted successfully.")

@click.command('delete-repo-all')
def delete_repository_all():
	response = RepositoryEngine.delete_repository_all()
	if response:
		click.echo("INFO: All repositories are deleted successfully.")
	else:
		click.echo("ERROR: All repositories are not deleted successfully.")


clustered_cli.add_command(setup_database)
clustered_cli.add_command(create_repository)
clustered_cli.add_command(get_repository_list)
clustered_cli.add_command(delete_repository)
clustered_cli.add_command(delete_repository_all)


if __name__ == '__main__':
	clustered_cli()





# @click.command('test-models')
# def test_database_models():
# 	repo_one = Repository(REPO_NAME='abc', REPO_ACCESS_KEY_ENCRYPTED='abc', REPO_SECRET_KEY_ENCRYPTED='def', REPO_STATE='limbo', REPO_ACTIVE_FLAG='N')
# 	cluster_one = Cluster(CLUSTER_NAME='cluster_one', CLUSTER_STATE = 'limbo', CLUSTER_ACTIVE_FLAG = 'N', REPOSITORY = repo_one)
# 	cluster_two = Cluster(CLUSTER_NAME='cluster_two', CLUSTER_STATE = 'limbo', CLUSTER_ACTIVE_FLAG = 'N', REPOSITORY = repo_one)
# 	node_one = Node(NODE_NAME = 'node_one', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_one)
# 	node_two = Node(NODE_NAME = 'node_two', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_one)
# 	node_three = Node(NODE_NAME = 'node_three', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_two)
# 	node_four = Node(NODE_NAME = 'node_four', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_two)
# 	print(node_three)
# 	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
# 	print(node_two)
# 	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
# 	print(node_four.CLUSTER)
# 	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
# 	print(node_one.CLUSTER.REPOSITORY)