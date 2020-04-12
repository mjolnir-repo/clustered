#!/usr/bin/env python

import click
from clustered.env import env
from clustered.engine.repository_engine import RepositoryEngine
from clustered.models import Repository, Cluster, Node


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

@click.command('ls-repo')
@click.option('--env', default='dev', help='Execution environment.')
def get_repository_list(env):
	click.echo('Arg test: ' + env)
	repo_eng = RepositoryEngine()
	repo_eng.get_repositories()

@click.command('test-models')
def test_database_models():
	# print('~~~~~~~~~~~	Repository Details	~~~~~~~~~~~')
	repo_one = Repository(REPO_NAME='abc', REPO_ACCESS_KEY_ENCRYPTED='abc', REPO_SECRET_KEY_ENCRYPTED='def', REPO_STATE='limbo', REPO_ACTIVE_FLAG='N')
	# print(repo_one)
	# print('~~~~~~~~~~~	Cluster Details	~~~~~~~~~~~')
	cluster_one = Cluster(CLUSTER_NAME='cluster_one', CLUSTER_STATE = 'limbo', CLUSTER_ACTIVE_FLAG = 'N', REPOSITORY = repo_one)
	cluster_two = Cluster(CLUSTER_NAME='cluster_two', CLUSTER_STATE = 'limbo', CLUSTER_ACTIVE_FLAG = 'N', REPOSITORY = repo_one)
	# print(cluster_one)
	# print(cluster_two)
	# print(cluster_one.REPOSITORY)
	print('~~~~~~~~~~~	Node Details	~~~~~~~~~~~')
	node_one = Node(NODE_NAME = 'node_one', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_one)
	node_two = Node(NODE_NAME = 'node_two', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_one)
	node_three = Node(NODE_NAME = 'node_three', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_two)
	node_four = Node(NODE_NAME = 'node_four', NODE_TYPE = 'M', NODE_INSTANCE_TYPE = 't2.micro', NODE_KEY_PAIR_NAME = 'spark-cluster', NODE_BLOCK_DEVICE_MAPPING = 'a/b/c', NODE_STATE = 'limbo', NODE_ACTIVE_FLAG = 'N', CLUSTER = cluster_two)
	print(node_three)
	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
	print(node_two)
	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
	print(node_four.CLUSTER)
	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
	print(node_one.CLUSTER.REPOSITORY)


clustered_cli.add_command(get_repository_list)
clustered_cli.add_command(test_database_models)

if __name__ == '__main__':
	clustered_cli()
