"""
All cluster related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""
import pprint
import pickle
from ..models import Repository, Cluster, Node
from ..database import db_obj
from ..env import env
from ..exceptions import ClusterNotPresentError, ClusterAlreadyExistsError, RepositoryNotPresentError, EncryptorNotPresentError, UnavailableActionError
from ..encrypt import Encrypt
from sqlalchemy import and_, exc



class ClusterEngine:
    def __init__(self):
        pass


    @staticmethod
    def add_cluster(clus_name:str, repo_name:str, clus_conf_file:str) -> None:
        try:
            with db_obj.session_scope() as sess:
                repo_obj = sess.query(Repository)\
                    .filter(
                        and_(
                            Repository.REPO_ACTIVE_FLAG == 'Y'
                            , Repository.REPO_NAME == repo_name.upper()
                        )
                    ).first()
                if not repo_obj:
                    raise RepositoryNotPresentError(f"Repository<'{repo_name}'> is either not present or inactive.")
                #TO DO: Check cluster config file existence
                #TO DO: Read config file
                #TO DO: Write all AWS related Code here.
                clus_obj = Cluster(
                    CLUSTER_NAME=clus_name.upper(),
                    CLUSTER_DESC='#TO DO: Take from cluster config file.',
                    CLUSTER_SECURITY_GROUP_ID='#TO DO: Will be added later',
                    CLUSTER_WHITELISTED_IP_SET='#TO DO: Take from cluster config file',
                    CLUSTER_STATE='AVAILABLE',
                    CLUSTER_ACTIVE_FLAG='Y',
                    REPOSITORY = repo_obj
                )
                sess.add(clus_obj)
        except exc.IntegrityError as e:
            raise ClusterAlreadyExistsError()


    @staticmethod
    def describe_cluster(clus_name:str = '', repo_name:str = '') -> Cluster:
        with db_obj.session_scope() as sess:
            clus_obj = sess.query(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                    )
                ).first()
            if not clus_obj:
                raise ClusterNotPresentError()
            return clus_obj


    @staticmethod
    def list_clusters(repo_name:str = '') -> [Cluster]:
        with db_obj.session_scope() as session:
            if not repo_name:
                clus_list = [
                    {
                        'CLUSTER_NAME': clus.CLUSTER_NAME,
                        'CLUSTER_ACTIVE_FLAG': clus.CLUSTER_ACTIVE_FLAG
                    } for clus in session.query(Cluster)
            ]
            else:
                clus_list = [
                    {
                        'CLUSTER_NAME': clus.CLUSTER_NAME,
                        'CLUSTER_ACTIVE_FLAG': clus.CLUSTER_ACTIVE_FLAG
                    } for clus in 
                        session.query(Cluster)\
                            .join(Repository)\
                            .filter(
                                Repository.REPO_NAME == repo_name.upper()
                            )
                ]
        return clus_list


    @staticmethod
    def start_cluster(clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        print('DEBUG: Start Cluster feature is not available yet.')
        raise UnavailableActionError("'start-cluster' action is not available yet.")


    @staticmethod
    def stop_cluster(clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        print('DEBUG: Stop Cluster feature is not available yet.')
        raise UnavailableActionError("'stop-cluster' action is not available yet.")

    
    @staticmethod
    def delete_cluster(clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as session:
            clus_obj = session.query(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                ).first()
            clus_obj.CLUSTER_ACTIVE_FLAG == 'N'
            session.commit()

    
    @staticmethod
    def recover_cluster(clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as session:
            clus_obj = session.query(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                ).first()
            if clus_obj.CLUSTER_ACTIVE_FLAG == 'Y':
                raise WrongActionInvocationError(f"Cluster<'{clus_name}'> is already Active.")
            clus_obj.CLUSTER_ACTIVE_FLAG == 'Y'
            session.commit()


    @staticmethod
    def flush_clusters(repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as session:
            inactive_clus_id_list = [clus[0] for clus in session.query(Cluster.CLUSTER_ID)\
                .join(Repository)\
                .filter(
                    and_(
                        Repository.REPO_NAME == repo_name.upper()
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'N'
                    )
                )
            ]
            session.query(Cluster)\
                .filter(Cluster.CLUSTER_ID.in_(inactive_clus_id_list))\
                .delete(synchronize_session=False)


    @staticmethod
    def purge_all_clusters(repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as session:
            all_clus_id_list = [clus[0] for clus in session.query(Cluster.CLUSTER_ID)\
                .join(Repository)\
                .filter(
                    and_(
                        Repository.REPO_NAME == repo_name.upper()
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                )
            ]
            session.query(Cluster)\
                .filter(Cluster.CLUSTER_ID.in_(all_clus_id_list))\
                .delete(synchronize_session=False)
