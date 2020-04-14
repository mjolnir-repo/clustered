"""
All cluster related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""
import pprint
import pickle
from ..models import Repository, Cluster, Node
from ..database import db_obj
from ..env import env
from ..exceptions import ClusterNotPresentError, ClusterAlreadyExistsError, RepositoryNotPresentError, EncryptorNotPresentError
from ..encrypt import Encrypt
from sqlalchemy import and_, exc



class ClusterEngine:
    def __init__(self):
        pass


    @staticmethod
    def describe_cluster(clus_name:str = '') -> Cluster:
        with db_obj.session_scope() as sess:
            cluster = [clus for clus in sess.query(Cluster).filter(and_(Cluster.CLUSTER_ACTIVE_FLAG == 'Y', Cluster.CLUSTER_NAME == clus_name.upper()))]
            if cluster:
                return cluster[0]
            else:
                raise ClusterNotPresentError()


    @staticmethod
    def list_clusters() -> [Cluster]:
        with db_obj.session_scope() as session:
            clus_list = [clus for clus in session.query(Cluster).filter(Cluster.CLUSTER_ACTIVE_FLAG == 'Y')]
        return clus_list


    @staticmethod
    def create_cluster(clus_name:str, repo_name:str, clus_conf_file:str) -> bool:
        try:
            with db_obj.session_scope() as sess:
                repository = [repo for repo in sess.query(Repository).filter(and_(Repository.REPO_ACTIVE_FLAG == 'Y', Repository.REPO_NAME == repo_name.upper()))]
                if repository:
                    repo_obj = repository[0]
                else:
                    raise RepositoryNotPresentError()

                enc_obj = repo_obj.ENCRYPTOR
                if not enc_obj:
                    raise EncryptorNotPresentError()

            #TO DO: Check cluster config file existence
            #TO DO: Read config file
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                clus_obj = Cluster(
                    CLUSTER_NAME=clus_name.upper(),
                    CLUSTER_DESC='#TO DO: Take from cluster config file.',
                    CLUSTER_SECURITY_GROUP_ID='#TO DO: Will be added later',
                    CLUSTER_WHITELISTED_IP_SET='#TO DO: Take from cluster config file',
                    CLUSTER_STATE='AVAILABLE',
                    CLUSTER_ACTIVE_FLAG='Y',
                    REPOSITORY = repo_obj
                )
                session.add(clus_obj)
            return True
        except exc.IntegrityError as e:
            raise ClusterAlreadyExistsError()
        except Exception as e:
            print("ERROR: Exception occured while creating cluster: " + str(e))
            return False


    @staticmethod
    def start_cluster(clus_name:str) -> bool:
        #TO DO: Write all AWS related Code here.
        print('DEBUG: Start Cluster feature is not available yet.')
        return False


    @staticmethod
    def stop_cluster(clus_name:str) -> bool:
        #TO DO: Write all AWS related Code here.
        print('DEBUG: Stop Cluster feature is not available yet.')
        return False

    
    @staticmethod
    def delete_cluster(clus_name:str, repo_name:str) -> bool:
        try:
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                repo_id = session.query(Repository.REPO_ID).filter(and_(Repository.REPO_NAME == repo_name.upper(), Repository.REPO_ACTIVE_FLAG == 'Y')).first()[0]
                session.query(Cluster).filter(and_(Cluster.CLUSTER_NAME == clus_name.upper(), Cluster.CLUSTER_REPO_ID == repo_id, Cluster.CLUSTER_ACTIVE_FLAG == 'Y')).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while deleting cluster: " + str(e))
            return False


    @staticmethod
    def purge_all_clusters() -> bool:
        try:
            #TO DO: Write all AWS related Code here.
            with db_obj.session_scope() as session:
                session.query(Cluster).delete(synchronize_session=False)
            return True
        except Exception as e:
            print("ERROR: Exception occured while deleting clusters: " + str(e))
            return False
