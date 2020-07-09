#!/usr/bin/env python

import click
import pprint
import traceback
from clustered.engine.application_engine import ApplicationEngine
from clustered.engine.encryptor_engine import EncryptorEngine
from clustered.engine.repository_engine import RepositoryEngine
# from clustered.engine.cluster_engine import ClusterEngine
# from clustered.engine.node_engine import NodeEngine


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


##################################### Application CLI Commands #####################################

@click.command('init')
@click.option('-en', '--env-config-file', 'env_config_file', default='', help="Optional Environment configuration file. If not provided system will look for either CLUSTERED__ENVIRON_CONFIG_FILE os variable or config/Environment_Config.json file. If none of these are provided, initiate will fail.")
@click.option('-rp', '--repo-config-file', 'repo_config_file', default='', help="Optional Repository configuration file. If not provided system will look for either CLUSTERED__REPOSITORY_CONFIG_FILE os variable or config/Repository_Config.json file.")
@click.option('-cl', '--clus-config-file', 'clus_config_file', default='', help="Optional Cluster configuration file. If not provided system will look for either CLUSTERED__CLUSTER_CONFIG_FILE os variable or config/Cluster_Config.json file.")
@click.option('-pn', '--pnode-config-file', 'pnode_config_file', default='', help="Optional Parent Node configuration file. If not provided system will look for either CLUSTERED__PARENT_NODE_CONFIG_FILE os variable or config/Parent_Node_Config.json file.")
@click.option('-cn', '--cnode-config-file', 'cnode_config_file', default='', help="Optional Child Nodes configuration file. If not provided system will look for either CLUSTERED__CHILD_NODE_CONFIG_FILE os variable or config/Child_Node_Config.json file.")
def initiate(**kwargs):
	try:
		force = click.prompt(f"INPUT: Do you want to initiate CLUSTERED application(Y/N)?")
		if force.lower() == 'y':
			# TODO: User authentication
			app_engine = ApplicationEngine()
			app_engine.initiate(**kwargs)
			click.echo(f"INFO: CLUSTERED is initiated successfully.")
		else:
			click.echo("DEBUG: CLUSTERED initiation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while Initating the application: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"CLUSTERED initiation has Failed.")


@click.command('refresh')
@click.option('-rp', '--repo-config-file', 'repo_config_file', default='', help="Optional Repository configuration file. If not provided system will look for either CLUSTERED__REPOSITORY_CONFIG_FILE os variable or config/Repository_Config.json file.")
@click.option('-cl', '--clus-config-file', 'clus_config_file', default='', help="Optional Cluster configuration file. If not provided system will look for either CLUSTERED__CLUSTER_CONFIG_FILE os variable or config/Cluster_Config.json file.")
@click.option('-pn', '--pnode-config-file', 'pnode_config_file', default='', help="Optional Parent Node configuration file. If not provided system will look for either CLUSTERED__PARENT_NODE_CONFIG_FILE os variable or config/Parent_Node_Config.json file.")
@click.option('-cn', '--cnode-config-file', 'cnode_config_file', default='', help="Optional Child Nodes configuration file. If not provided system will look for either CLUSTERED__CHILD_NODE_CONFIG_FILE os variable or config/Child_Node_Config.json file.")
def refresh(**kwargs):
	try:
		force = click.prompt(f"INPUT: Do you want to refresh CLUSTERED configuration(Y/N)?")
		if force.lower() == 'y':
			# TODO: User authentication
			app_engine = ApplicationEngine()
			app_engine.refresh(**kwargs)
			click.echo(f"INFO: CLUSTERED configurations are refreshed successfully.")
		else:
			click.echo("DEBUG: CLUSTERED configuration refresh is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while refreshing the configurations: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"CLUSTERED configuration refresh has Failed.")


@click.command('destroy')
def destroy():
	try:
		force = click.prompt(f"INPUT: Do you want to detroy CLUSTERED application from the system(Y/N)?")
		if force.lower() == 'y':
			# TODO: User authentication
			app_engine = ApplicationEngine()
			app_engine.destroy()
			click.echo(f"INFO: BOOM!!!!")
		else:
			click.echo("DEBUG: CLUSTERED destruction is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while destroying the application: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"CLUSTERED destruction has Failed.")



clustered_cli.add_command(initiate)
clustered_cli.add_command(refresh)
clustered_cli.add_command(destroy)


###################################### Encryptor CLI Commands ######################################

@click.command('add-enc')
@click.argument('enc_name', type=str, nargs=1)
def add_encryptor(enc_name):
	try:
		resp = EncryptorEngine.add_encryptor(enc_name)
		click.echo(f"INFO: Encryptor<'{enc_name}'> is added successfully.")
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"Encryptor<'{enc_name}'> is not added.")


@click.command('desc-enc')
@click.argument('enc_name', type=str, nargs=1)
def describe_encryptor(enc_name):
	try:
		resp = EncryptorEngine.describe_encryptor(enc_name)
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting requested encryptor: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)


@click.command('ls-encs')
def list_encryptors():
	try:
		resp = EncryptorEngine.list_encryptors()
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)


@click.command('delete-enc')
@click.argument('enc_name', type=str, nargs=1)
def delete_encryptor(enc_name):
	try:
		force = click.prompt(f"INPUT: Are you sure to delete '{enc_name}' encryptor(Y/N)?")
		if force.lower() == 'y':
			resp = EncryptorEngine.delete_encryptor(enc_name)
			click.echo(f"INFO: Encryptor<'{enc_name}'> is deleted successfully.")
		else:
			click.echo("DEBUG: Delete operation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while deleting requested encryptor: " + str(e))
		click.echo(f"ERROR: Encryptor<'{enc_name}'> is not deleted.")
	try:
		resp = EncryptorEngine.list_encryptors()
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting encryptor list: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)


@click.command('recover-enc')
@click.argument('enc_name', type=str, nargs=1)
def recover_encryptor(enc_name):
	try:
		force = click.prompt(f"INPUT: Are you sure to recover '{enc_name}' encryptor(Y/N)?")
		if force.lower() == 'y':
			resp = EncryptorEngine.recover_encryptor(enc_name)
			click.echo(f"INFO: Encryptor<'{enc_name}'> is recovered successfully.")
		else:
			click.echo("DEBUG: Recovery operation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while recovering requested encryptor: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"ERROR: Encryptor<'{enc_name}'> is not recovered.")


@click.command('flush-encs')
def flush_encryptors():
	try:
		force = click.prompt("INPUT: Do you want to clear inactive encryptors(Y/N)?")
		if force.lower() == 'y':
			resp = EncryptorEngine.flush_encryptors()
			click.echo("INFO: All inactive encryptors are cleared successfully.")
		else:
			click.echo("DEBUG: Flush operation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while deleting inactive encryptors: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo("ERROR: All inactive encryptors are not cleared.")


@click.command('purge-all-encs')
def purge_all_encryptors():
	try:
		force = click.prompt("INPUT: Do you want to clear all encryptors(Y/N)?")
		if force.lower() == 'y':
			resp = EncryptorEngine.purge_all_encryptors()
			click.echo("INFO: All encryptors are purged successfully.")
		else:
			click.echo("DEBUG: Purge operation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while purging all encryptor: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo("ERROR: All encryptors are not purged successfully.")



clustered_cli.add_command(add_encryptor)
clustered_cli.add_command(describe_encryptor)
clustered_cli.add_command(list_encryptors)
clustered_cli.add_command(delete_encryptor)
clustered_cli.add_command(recover_encryptor)
clustered_cli.add_command(flush_encryptors)
clustered_cli.add_command(purge_all_encryptors)


###################################### Repository CLI Commands ######################################

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

		resp = RepositoryEngine.add_repository(repo_name, enc_name, aws_access_key, aws_secret_key, aws_region)
		click.echo(f"INFO: Repository<'{repo_name}'> is created successfully.")
	except Exception as e:
		click.echo("ERROR: Exception occured while adding repository: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"ERROR: Repository<'{repo_name}'> is not created.")


@click.command('desc-repo')
@click.argument('repo_name', type=str, nargs=1)
def describe_repository(repo_name):
	try:
		resp = RepositoryEngine.describe_repository(repo_name)
		click.echo(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting requested repository: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)


@click.command('ls-repos')
def list_repositories():
	try:
		resp = RepositoryEngine.list_repositories()
		print(resp)
	except Exception as e:
		click.echo("ERROR: Exception occured while extracting all repositories: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)


@click.command('delete-repo')
@click.argument('repo_name', type=str, nargs=1)
def delete_repository(repo_name):
	try:
		force = click.prompt(f"INPUT: Are you sure to delete Repository<'{repo_name}'>(Y/N)?")
		if force.lower() == 'y':
			resp = RepositoryEngine.delete_repository(repo_name)
			click.echo(f"INFO: Repository<'{repo_name}'> is deleted successfully.")
		else:
			click.echo("DEBUG: Delete operation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while deleting requested repository: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"ERROR: Repository<'{repo_name}'> is not deleted.")


@click.command('recover-repo')
@click.argument('repo_name', type=str, nargs=1)
def recover_repository(repo_name):
	try:
		force = click.prompt(f"INPUT: Are you sure to recover Repository<'{repo_name}'>(Y/N)?")
		if force.lower() == 'y':
			resp = RepositoryEngine.recover_repository(repo_name)
			click.echo(f"INFO: Repository<'{repo_name}'> is recovered successfully.")
		else:
			click.echo("DEBUG: Recovery operation is skipped.")
	except Exception as e:
		click.echo("ERROR: Exception occured while recobering requested repository: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo(f"ERROR: Repository<'{repo_name}'> is not recovered.")


@click.command('flush-repos')
def flush_repositories():
	try:
		force = click.prompt(f"INPUT: Do you want to clear inactive repositories(Y/N)?")
		if force.lower() == 'y':
			resp = RepositoryEngine.flush_repositories()
			click.echo("INFO: All inactive repositories are cleared successfully.")
		else:
			click.echo("DEBUG: Flush operation is skipped.")
	except Exception as e:
		print("ERROR: Exception occured while clearing inactive repositories: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo("ERROR: All inactive repositories are not deleted.")


@click.command('purge-all-repos')
def purge_all_repositories():
	try:
		force = click.prompt(f"INPUT: Do you want to clear all repositories(Y/N)?")
		if force.lower() == 'y':
			resp = RepositoryEngine.purge_all_repositories()
			click.echo("INFO: All repositories are purged successfully.")
		else:
			click.echo("DEBUG: Purge operation is skipped.")
	except Exception as e:
		print("ERROR: Exception occured while purging all repository: " + str(e))
		click.echo('~' * 100)
		traceback.print_exc()
		click.echo('~' * 100)
		click.echo("ERROR: All repositories are not purged successfully.")



clustered_cli.add_command(add_repository)
clustered_cli.add_command(describe_repository)
clustered_cli.add_command(list_repositories)
clustered_cli.add_command(delete_repository)
clustered_cli.add_command(recover_repository)
clustered_cli.add_command(flush_repositories)
clustered_cli.add_command(purge_all_repositories)


# ###################################### Cluster CLI Commands ######################################

# @click.command('add-cluster')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# @click.option('-f', '--cluster-config-file', 'clus_conf_file', default='', help="Cluster configuration file, if not present, default configurations will be used from config/Cluster_Config.json.")
# def add_cluster(clus_name, repo_name, **kwargs):
# 	try:
# 		clus_conf_file = kwargs.get('clus_conf_file', '')
# 		if not clus_conf_file:
# 			clus_conf_file = env.CLUSTER_CONFIG_FILE
# 			click.echo("DEBUG: Cluster configuration file is not provided, using default cluster configuration file: " + clus_conf_file)
# 		response = ClusterEngine.add_cluster(clus_name, repo_name, clus_conf_file)
# 		click.echo(f"INFO: Cluster<'{clus_name}'> is created successfully.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while adding cluster: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Cluster<'{clus_name}'> is not created.")


# @click.command('desc-cluster')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def describe_cluster(clus_name, repo_name):
# 	try:
# 		resp = ClusterEngine.describe_cluster(clus_name, repo_name)
# 		click.echo(resp)
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while extracting requested cluster: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)


# @click.command('ls-clusters')
# @click.argument('repo_name', type=str, nargs=1)
# def list_clusters(repo_name):
# 	try:
# 		resp = ClusterEngine.list_clusters(repo_name)
# 		print(resp)
# 	except Exception as e:
# 		click.echo(f"ERROR: Exception occured while extracting all clusters of Repository<'{repo_name}'>: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)


# @click.command('start-cluster')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def start_cluster(clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Start Cluster<'{clus_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			resp = ClusterEngine.start_cluster(clus_name, repo_name)
# 			click.echo(f"INFO: Cluster<'{clus_name}'> is running now.")
# 		else:
# 			click.echo("DEBUG: Start cluster command is cancelled.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while starting cluster: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Cluster<'{clus_name}'> could not be started.")


# @click.command('stop-cluster')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def stop_cluster(clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Stop Cluster<'{clus_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			resp = ClusterEngine.stop_cluster(clus_name, repo_name)
# 			click.echo(f"INFO: Cluster<'{clus_name}'> is stopped now.")
# 		else:
# 			click.echo("DEBUG: Stop cluster command is cancelled.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while stopping cluster: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Cluster<'{clus_name}'> could not be stopped.")


# @click.command('delete-cluster')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def delete_cluster(clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Are you sure to delete '{clus_name}' cluster(Y/N)?")
# 		if force.lower() == 'y':
# 			response = ClusterEngine.delete_cluster(clus_name, repo_name)
# 			click.echo(f"INFO: Cluster<'{clus_name}'> is deleted successfully.")
# 		else:
# 			click.echo("DEBUG: Delete operation is skipped.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while deleting cluster: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Cluster<'{clus_name}'> is not deleted.")


# @click.command('recover-cluster')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def recover_cluster(clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Are you sure to recover '{clus_name}' cluster(Y/N)?")
# 		if force.lower() == 'y':
# 			response = ClusterEngine.recover_cluster(clus_name, repo_name)
# 			click.echo(f"INFO: Cluster<'{clus_name}'> is recovered successfully.")
# 		else:
# 			click.echo("DEBUG: Recovery operation is skipped.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while recovering cluster: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Cluster<'{clus_name}'> is not recovered.")


# @click.command('flush-clusters')
# @click.argument('repo_name', type=str, nargs=1)
# def flush_clusters(repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Do you want to clear all inactive clusters of Repository<'{repo_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			response = ClusterEngine.flush_clusters(repo_name)
# 			click.echo(f"INFO: All inactive clusters of Repository<'{repo_name}'> are cleared successfully.")
# 		else:
# 			click.echo(f"DEBUG: Purge operation is skipped.")
# 	except Exception as e:
# 		click.echo(f"ERROR: Exception occured while deleting all inactive clusters from Repository<'{repo_name}'>: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: All inactive clusters of Repository<'{repo_name}'> are not cleared.")


# @click.command('purge-all-clusters')
# @click.argument('repo_name', type=str, nargs=1)
# def purge_all_clusters(repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Do you want to clear all clusters of Repository<'{repo_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			response = ClusterEngine.purge_all_clusters(repo_name)
# 			click.echo(f"INFO: All clusters of Repository<'{repo_name}'> are purged successfully.")
# 		else:
# 			click.echo(f"DEBUG: Purge operation is skipped.")
# 	except Exception as e:
# 		click.echo(f"ERROR: Exception occured while deleting all clusters from Repository<'{repo_name}'>: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: All clusters of Repository<'{repo_name}'> are not purged.")



# clustered_cli.add_command(add_cluster)
# clustered_cli.add_command(describe_cluster)
# clustered_cli.add_command(list_clusters)
# clustered_cli.add_command(start_cluster)
# clustered_cli.add_command(stop_cluster)
# clustered_cli.add_command(delete_cluster)
# clustered_cli.add_command(recover_cluster)
# clustered_cli.add_command(flush_clusters)
# clustered_cli.add_command(purge_all_clusters)


# ###################################### Node CLI Commands ######################################

# @click.command('add-master-node')
# @click.argument('node_name', type=str, nargs=1)
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# @click.option('-n', '--node-config-file', 'node_conf_file', default='', help="Node configuration file, if not present, default configurations will be used from config/Master_Node_Config.json.")
# def add_master_node(node_name, clus_name, repo_name, **kwargs):
# 	try:
# 		node_conf_file = kwargs.get('node_conf_file', '')
# 		if node_conf_file:
# 			pass
# 		else:
# 			node_conf_file = env.MASTER_NODE_CONFIG_FILE
# 			click.echo("DEBUG: Master Node configuration file is not provided, using default master node configuration file: " + node_conf_file)

# 		resp = NodeEngine.create_node(node_name, 'M', clus_name, repo_name, node_conf_file)
# 		click.echo(f"INFO: Node<'{node_name}'> is created successfully.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while adding node: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Node<'{node_name}'> is not created.")


# @click.command('add-slave-node')
# @click.argument('node_name', type=str, nargs=1)
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# @click.option('-n', '--node-config-file', 'node_conf_file', default='', help="Node configuration file, if not present, default configurations will be used from config/Slave_Node_Config.json.")
# def add_slave_node(node_name, clus_name, repo_name, **kwargs):
# 	try:
# 		node_conf_file = kwargs.get('node_conf_file', '')
# 		if node_conf_file:
# 			pass
# 		else:
# 			node_conf_file = env.SLAVE_NODE_CONFIG_FILE
# 			click.echo("DEBUG: Slave Node configuration file is not provided, using default slave node configuration file: " + node_conf_file)

# 		resp = NodeEngine.create_node(node_name, 'S', clus_name, repo_name, node_conf_file)
# 		click.echo(f"INFO: Node<'{node_name}'> is created successfully.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while adding node: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Node<'{node_name}'> is not created.")


# @click.command('desc-node')
# @click.argument('node_name', type=str, nargs=1)
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def describe_node(node_name, clus_name, repo_name):
# 	try:
# 		resp = NodeEngine.describe_node(node_name, clus_name, repo_name)
# 		click.echo(resp)
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while extracting requested node: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)


# @click.command('ls-nodes')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def list_nodes(clus_name, repo_name):
# 	try:
# 		resp = NodeEngine.list_nodes(clus_name, repo_name)
# 		print(resp)
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while listing requested nodes: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)


# @click.command('delete-node')
# @click.argument('node_name', type=str, nargs=1)
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def delete_node(node_name, clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Are you sure to delete Node<'{node_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			resp = NodeEngine.delete_node(node_name, clus_name, repo_name)
# 			click.echo(f"INFO: Node<'{node_name}'> is deleted successfully.")
# 		else:
# 			click.echo("DEBUG: Delete operation is skipped.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while deleting node: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Node<'{node_name}'> is not deleted.")


# @click.command('recover-node')
# @click.argument('node_name', type=str, nargs=1)
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def recover_node(node_name, clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Are you sure to recover Node<'{node_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			resp = NodeEngine.recover_node(node_name, clus_name, repo_name)
# 			click.echo(f"INFO: Node<'{node_name}'> is recovered successfully.")
# 		else:
# 			click.echo("DEBUG: Recovery operation is skipped.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while recovering node: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: Node<'{node_name}'> is not recovered.")


# @click.command('flush-nodes')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def flush_nodes(clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Do you want to clear all inactive nodes in Cluster<'{clus_name}'> of Repository<'{repo_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			resp = NodeEngine.flush_nodes(clus_name, repo_name)
# 			click.echo(f"INFO: All inactive nodes in Cluster<'{clus_name}'> of Repository<'{repo_name}'> are purged successfully.")
# 		else:
# 			click.echo("DEBUG: Purge operation is skipped.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while deleting all inactive nodes: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: All inactive nodes in Cluster<'{clus_name}'> of Repository<'{repo_name}'> are not deleted.")


# @click.command('purge-all-nodes')
# @click.argument('clus_name', type=str, nargs=1)
# @click.argument('repo_name', type=str, nargs=1)
# def purge_all_nodes(clus_name, repo_name):
# 	try:
# 		force = click.prompt(f"INPUT: Do you want to clear all nodes in Cluster<'{clus_name}'> of Repository<'{repo_name}'>(Y/N)?")
# 		if force.lower() == 'y':
# 			resp = NodeEngine.purge_all_nodes(clus_name, repo_name)
# 			click.echo(f"INFO: All nodes in Cluster<'{clus_name}'> of Repository<'{repo_name}'> are purged successfully.")
# 		else:
# 			click.echo("DEBUG: Purge operation is skipped.")
# 	except Exception as e:
# 		click.echo("ERROR: Exception occured while prging all nodes: " + str(e))
# 		click.echo('~' * 100)
# 		traceback.print_exc()
# 		click.echo('~' * 100)
# 		click.echo(f"ERROR: All nodes in Cluster<'{clus_name}'> of Repository<'{repo_name}'> are not purged.")



# clustered_cli.add_command(add_master_node)
# clustered_cli.add_command(add_slave_node)
# clustered_cli.add_command(describe_node)
# clustered_cli.add_command(list_nodes)
# clustered_cli.add_command(delete_node)
# clustered_cli.add_command(recover_node)
# clustered_cli.add_command(flush_nodes)
# clustered_cli.add_command(purge_all_nodes)


###################################### Bootup CLI application ######################################

if __name__ == '__main__':
	clustered_cli()
