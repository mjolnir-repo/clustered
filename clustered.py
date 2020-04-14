#!/usr/bin/env python

import click
import pprint
from clustered.env import env
from clustered.engine.encryptor_engine import EncryptorEngine
from clustered.engine.repository_engine import RepositoryEngine
from clustered.engine.cluster_engine import ClusterEngine
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


###################################### Database CLI Commands ######################################

@click.command('setup-db')
def setup_database():
	force = click.prompt("DEBUG:Do you really want to setup empty database(Y/N)?", type=str)
	if force.lower() == 'y':
		Base.metadata.create_all(db_obj.db_engine)
		click.echo("INFO: Database set up is completed.")
	else:
		click.echo("INFO: Database set up is cancelled.")


@click.command('cleanup-db')
def cleanup_database():
	force = click.prompt("DEBUG:Do you really want to drop all tables from database(Y/N)?", type=str)
	if force.lower() == 'y':
		Base.metadata.drop_all(db_obj.db_engine)
		click.echo("INFO: Database clean up is completed.")
	else:
		click.echo("DEDUB: Database clean up is cancelled.")


@click.command('purge-db')
def purge_database():
	force = click.prompt("DEBUG:Do you really want to delete all records from all tables(Y/N)?", type=str)
	if force.lower() == 'y':
		clus_res = ClusterEngine.purge_all_clusters()
		if clus_res:
			click.echo("INFO: All clusters are purged successfully.")
		else:
			click.echo("ERROR: All clusters are not purged.")

		repo_res = RepositoryEngine.purge_all_repositories()
		if repo_res:
			click.echo("INFO: All repositories are purged successfully.")
		else:
			click.echo("ERROR: All repositories are not purged successfully.")

		enc_res = EncryptorEngine.purge_all_encryptors()
		if enc_res:
			click.echo("INFO: All encryptors are purged successfully.")
		else:
			click.echo("ERROR: All encryptors are not purged successfully.")
		click.echo("INFO: Database purge is completed.")
	else:
		click.echo("DEDUB: Database purge is cancelled.")


clustered_cli.add_command(setup_database)
clustered_cli.add_command(cleanup_database)
clustered_cli.add_command(purge_database)


###################################### Encryptor CLI Commands ######################################

@click.command('desc-enc')
@click.argument('enc_name', type=str, nargs=1)
def describe_encryptor(enc_name):
	try:
		resp = EncryptorEngine.describe_encryptor(enc_name)
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting requested encryptor: " + str(e))


@click.command('ls-encs')
def list_encryptors():
	try:
		resp = EncryptorEngine.list_encryptors()
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))


@click.command('add-enc')
@click.argument('enc_name', type=str, nargs=1)
def add_encryptor(enc_name):
	try:
		resp = EncryptorEngine.add_encryptor(enc_name)
		if resp:
			click.echo("INFO: Encryptor<'" + enc_name + "'> is added successfully.")
		else:
			raise Exception("Encryptor<'" + enc_name + "'> is not added.")
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))


@click.command('delete-enc')
@click.argument('enc_name', type=str, nargs=1)
def delete_encryptor(enc_name):
	force = click.prompt("Are you sure to delete '" + enc_name + "' encryptor(Y/N)?")
	if force.lower() == 'y':
		response = EncryptorEngine.delete_encryptor(enc_name)
		if response:
			click.echo("INFO: Encryptor<'" + enc_name + "'> is deleted successfully.")
		else:
			click.echo("ERROR: Encryptor<'" + enc_name + "'> is not deleted successfully.")
	else:
		click.echo("DEBUG: Delete operation is skipped.")


@click.command('purge-all-encs')
def purge_all_encryptors():
	force = click.prompt("Do you want to clear all encryptors(Y/N)?")
	if force.lower() == 'y':
		response = EncryptorEngine.purge_all_encryptors()
		if response:
			click.echo("INFO: All encryptors are purged successfully.")
		else:
			click.echo("ERROR: All encryptors are not purged successfully.")
	else:
		click.echo("DEBUG: Purge operation is skipped.")



clustered_cli.add_command(describe_encryptor)
clustered_cli.add_command(list_encryptors)
clustered_cli.add_command(add_encryptor)
clustered_cli.add_command(delete_encryptor)
clustered_cli.add_command(purge_all_encryptors)


###################################### Repository CLI Commands ######################################

@click.command('desc-repo')
@click.argument('repo_name', type=str, nargs=1)
def describe_repository(repo_name):
	try:
		resp = RepositoryEngine.describe_repository(repo_name)
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting requested repository: " + str(e))


@click.command('add-repo')
@click.argument('repo_name', type=str, nargs=1)
@click.argument('enc_name', type=str, nargs=1)
@click.option('-a', '--aws-access-key', 'aws_access_key', default='', help="Optional access key of AWS account, if not provided, env value will be used.")
@click.option('-r', '--aws-region', 'aws_region', default='', help="Optional region of AWS account, if not provided, env value will be used.")
def add_repository(repo_name, enc_name, **kwargs):
	try:
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

		response = RepositoryEngine.create_repository(repo_name, enc_name, aws_access_key, aws_secret_key, aws_region)
		if response:
			click.echo("INFO: Repository<'" + repo_name + "'> is created successfully.")
		else:
			click.echo("ERROR: Repository<'" + repo_name + "'> is not created.")
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))


@click.command('ls-repos')
def list_repositories():
	resp = RepositoryEngine.list_repositories()
	print(resp)


@click.command('delete-repo')
@click.argument('repo_name', type=str, nargs=1)
def delete_repository(repo_name):
	force = click.prompt("Are you sure to delete '" + repo_name + "' repository(Y/N)?")
	if force.lower() == 'y':
		response = RepositoryEngine.delete_repository(repo_name)
		if response:
			click.echo("INFO: Repository<'" + repo_name + "'> is deleted successfully.")
		else:
			click.echo("ERROR: Repository<'" + repo_name + "'> is not deleted successfully.")
	else:
		click.echo("DEBUG: Delete operation is skipped.")


@click.command('purge-all-repos')
def purge_all_repositories():
	force = click.prompt("Do you want to clear all repositories(Y/N)?")
	if force.lower() == 'y':
		response = RepositoryEngine.purge_all_repositories()
		if response:
			click.echo("INFO: All repositories are purged successfully.")
		else:
			click.echo("ERROR: All repositories are not purged successfully.")
	else:
		click.echo("DEBUG: Purge operation is skipped.")



clustered_cli.add_command(describe_repository)
clustered_cli.add_command(add_repository)
clustered_cli.add_command(list_repositories)
clustered_cli.add_command(delete_repository)
clustered_cli.add_command(purge_all_repositories)


###################################### Repository CLI Commands ######################################

@click.command('desc-cluster')
@click.argument('clus_name', type=str, nargs=1)
def describe_cluster(clus_name):
	try:
		resp = ClusterEngine.describe_cluster(clus_name)
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting requested repository: " + str(e))


@click.command('add-cluster')
@click.argument('clus_name', type=str, nargs=1)
@click.argument('repo_name', type=str, nargs=1)
@click.option('-a', '--cluster-config-file', 'clus_conf_file', default='', help="Cluster configuration file, if not present, default configurations will be used from config/Cluster_Config.json.")
def add_cluster(clus_name, repo_name, **kwargs):
	try:
		clus_conf_file = kwargs.get('clus_conf_file', '')
		if clus_conf_file:
			pass
		else:
			clus_conf_file = env.APP_PATH + "/clustered/config/Cluster_Config.json"
			click.echo("DEBUG: Cluster configuration file is not provided, using default cluster configuration file: " + clus_conf_file)

		response = ClusterEngine.create_cluster(clus_name, repo_name, clus_conf_file)
		if response:
			click.echo("INFO: Cluster<'" + clus_name + "'> is created successfully.")
		else:
			click.echo("ERROR: Cluster<'" + clus_name + "'> is not created successfully.")
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))


@click.command('ls-clusters')
def list_clusters():
	resp = ClusterEngine.list_clusters()
	print(resp)


@click.command('start-cluster')
@click.argument('clus_name', type=str, nargs=1)
def start_cluster(clus_name):
	try:
		force = click.prompt("Start Cluster<'" + clus_name + "'>(Y/N)?")
		if force.lower() == 'y':
			resp = ClusterEngine.start_cluster(clus_name)
			if resp:
				click.echo("INFO: Cluster<'" + clus_name + "'> is running now.")
			else:
				click.echo("ERROR: Cluster<'" + clus_name + "'> could not be started.")
		else:
			click.echo("DEBUG: Start cluster command is cancelled.")
	except Exception as e:
		click.echo("ERROR: Exception occured while starting cluster: " + str(e))


@click.command('stop-cluster')
@click.argument('clus_name', type=str, nargs=1)
def stop_cluster(clus_name):
	try:
		force = click.prompt("Stop Cluster<'" + clus_name + "'>(Y/N)?")
		if force.lower() == 'y':
			resp = ClusterEngine.stop_cluster(clus_name)
			if resp:
				click.echo("INFO: Cluster<'" + clus_name + "'> is stopped now.")
			else:
				click.echo("ERROR: Cluster<'" + clus_name + "'> could not be stopped.")
		else:
			click.echo("DEBUG: Stop cluster command is cancelled.")
	except Exception as e:
		click.echo("ERROR: Exception occured while stopping cluster: " + str(e))


@click.command('delete-cluster')
@click.argument('clus_name', type=str, nargs=1)
@click.argument('repo_name', type=str, nargs=1)
def delete_cluster(clus_name, repo_name):
	force = click.prompt("Are you sure to delete '" + clus_name + "' cluster(Y/N)?")
	if force.lower() == 'y':
		response = ClusterEngine.delete_cluster(clus_name, repo_name)
		if response:
			click.echo("INFO: Cluster<'" + clus_name + "'> is deleted successfully.")
		else:
			click.echo("ERROR: Cluster<'" + clus_name + "'> is not deleted.")
	else:
		click.echo("DEBUG: Delete operation is skipped.")


@click.command('purge-all-clusters')
def purge_all_clusters():
	force = click.prompt("Do you want to clear all clusters(Y/N)?")
	if force.lower() == 'y':
		response = ClusterEngine.purge_all_clusters()
		if response:
			click.echo("INFO: All clusters are purged successfully.")
		else:
			click.echo("ERROR: All clusters are not purged.")
	else:
		click.echo("DEBUG: Purge operation is skipped.")



clustered_cli.add_command(describe_cluster)
clustered_cli.add_command(add_cluster)
clustered_cli.add_command(list_clusters)
clustered_cli.add_command(start_cluster)
clustered_cli.add_command(stop_cluster)
clustered_cli.add_command(delete_cluster)
clustered_cli.add_command(purge_all_clusters)


###################################### Bootup CLI application ######################################

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