"""
All cluster related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""
import pprint
import pickle
from ..models import Repository, Cluster, Node
from ..database import db_obj
from ..env import env
from ..exceptions import ClusterNotPresentError, ClusterAlreadyExistsError, RepositoryNotPresentError, EncryptorNotPresentError, NodeAlreadyExistsError, NodeNotPresentError, MasterNodeAlreadyExistsError, MasterNodeNotPresentError, WrongActionInvocationError
from ..encrypt import Encrypt
from sqlalchemy import and_, exc, func



class NodeEngine:
    def __init__(self):
        pass


    @staticmethod
    def create_node(node_name:str, node_type:str, clus_name, repo_name:str, node_conf_file:str) -> None:
        try:
            with db_obj.session_scope() as sess:
                repo_obj = sess.query(func.count(Repository.REPO_ID))\
                    .filter(
                        and_(
                            Repository.REPO_ACTIVE_FLAG == 'Y'
                            , Repository.REPO_NAME == repo_name.upper()
                        )
                    ).first()[0]
                if not repo_obj:
                    raise RepositoryNotPresentError(f"Repository<'{repo_name}'> is either not present or inactive.")

                clus_obj = sess.query(Cluster)\
                    .join(Repository)\
                    .filter(
                        and_(
                            Repository.REPO_ACTIVE_FLAG == 'Y'
                            , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                            , Repository.REPO_NAME == repo_name.upper()
                            , Cluster.CLUSTER_NAME == clus_name.upper()
                        )
                    ).first()
                if not clus_obj:
                    raise ClusterNotPresentError(f"Cluster<'{clus_name}'> is either not present or inactive.")
                elif clus_obj.CLUSTER_STATE == 'RUNNING':
                    raise WrongActionInvocationError("Node can not be added while cluster is in RUNNING state. Please stop the cluster before attaching new node.")
                elif clus_obj.CLUSTER_STATE != 'AVAILABLE':
                    raise WrongActionInvocationError("Node can not be added while cluster is not in AVAILABLE state.")
                master_node_count = sess.query(func.count(Node.NODE_ID))\
                    .join(Cluster)\
                    .join(Repository)\
                    .filter(
                        and_(
                            Repository.REPO_NAME == repo_name.upper()
                            , Cluster.CLUSTER_NAME == clus_name.upper()
                            , Node.NODE_TYPE == 'M'
                            , Repository.REPO_ACTIVE_FLAG == 'Y'
                            , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                            , Node.NODE_ACTIVE_FLAG == 'Y'
                        )
                    ).first()[0]
                if node_type == 'M' and master_node_count:
                    raise MasterNodeAlreadyExistsError(f"One master node already exists in Cluster<'{clus_name}'> of Repository<'{repo_name}'>. Only one master node is allowed per cluster.")
                elif node_type == 'S' and (not master_node_count):
                    raise MasterNodeNotPresentError(f"There is no master node in Cluster<'{clus_name}'> of Repository<'{repo_name}'>. Please add master node before adding slave node.")

                    # #TO DO: Check Node config file existence
                    # #TO DO: Read config file
                    # #TO DO: Write all AWS related Code here.
                node_obj = Node(
                    NODE_NAME=node_name.upper(),
                    NODE_DESC='#TO DO: Take from cluster config file.',
                    NODE_TYPE=node_type,
                    NODE_INSTANCE_TYPE='#TO DO: Take from cluster config file.',
                    NODE_KEY_PAIR_NAME='#TO DO: Take from cluster config file.',
                    NODE_BLOCK_DEVICE_MAPPING='#TO DO: Take from cluster config file.',
                    NODE_STATE='AVAILABLE',
                    NODE_ACTIVE_FLAG='Y',
                    CLUSTER = clus_obj
                )
                sess.add(node_obj)
        except exc.IntegrityError as e:
            raise NodeAlreadyExistsError()
        # except Exception as e:
        #     print(str(e))


    @staticmethod
    def describe_node(node_name:str, clus_name:str, repo_name:str) -> Node:
        with db_obj.session_scope() as sess:
            node = sess.query(Node)\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Node.NODE_NAME == node_name.upper()
                        , Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                    )
                ).first()
            if not node:
                raise NodeNotPresentError()
            return node


    @staticmethod
    def list_nodes(clus_name:str, repo_name:str) -> [Cluster]:
        try:
            with db_obj.session_scope() as session:
                node_list = [
                    {
                        'NODE_NAME': node.NODE_NAME,
                        'NODE_ACTIVE_FLAG': node.NODE_ACTIVE_FLAG
                    } for node in
                        session.query(Node)\
                            .join(Cluster)\
                            .join(Repository)\
                            .filter(
                                and_(
                                    Repository.REPO_NAME == repo_name.upper()
                                    , Cluster.CLUSTER_NAME == clus_name.upper()
                                )
                            )
                ]
            return node_list
        except Exception as e:
            raise Execption(str(e))

    
    @staticmethod
    def delete_node(node_name:str, clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as sess:
            node = sess.query(Node)\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Node.NODE_NAME == node_name.upper()
                        , Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Node.NODE_ACTIVE_FLAG == 'Y'
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                ).first()
            if node.NODE_TYPE == 'M':
                slave_count = sess.query(func.count(Node.NODE_ID))\
                    .join(Cluster)\
                    .join(Repository)\
                    .filter(
                        and_(
                            Node.NODE_TYPE == 'S'
                            , Cluster.CLUSTER_NAME == clus_name.upper()
                            , Repository.REPO_NAME == repo_name.upper()
                            , Node.NODE_ACTIVE_FLAG == 'Y'
                            , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                            , Repository.REPO_ACTIVE_FLAG == 'Y'
                        )
                    ).first()[0]
                if slave_count:
                    raise Exception(f"There are active slave nodes present in Cluster<'{clus_name}'> of Repository<'{repo_name}'>. Master node can not be deleted while there are active slave nodes in cluster.")
            node_obj = sess.query(Node)\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Node.NODE_NAME == node_name.upper()
                        , Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Node.NODE_ACTIVE_FLAG == 'Y'
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                ).first()
            node_obj.NODE_ACTIVE_FLAG = 'N'
            sess.commit()

    
    @staticmethod
    def recover_node(node_name:str, clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as sess:
            node_obj = sess.query(Node)\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Node.NODE_NAME == node_name.upper()
                        , Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Node.NODE_ACTIVE_FLAG == 'N'
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                ).first()
            if node_obj.CLUSTER_ACTIVE_FLAG == 'Y':
                raise WrongActionInvocationError(f"Node<'{node_name}'> is already Active.")
            master_count = sess.query(func.count(node_obj.NODE_ID))\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Node.NODE_TYPE == 'M'
                        , Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Node.NODE_ACTIVE_FLAG == 'Y'
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                ).first()[0]
            if node_obj.NODE_TYPE == 'M' and master_count:
                raise MasterNodeAlreadyExistsError(f"There is already active master node present in Cluster<'{clus_name}'> of Repository<'{repo_name}'>. Only one master node allowed per cluster.")
            elif node_obj.NODE_TYPE == 'S' and (not master_count):
                raise MasterNodeNotPresentError(f"ERROR: There is no active master node present in Cluster<'{clus_name}'> of Repository<'{repo_name}'>. Atleast one master node is required before slave nodes can be present.")
            node_obj.NODE_ACTIVE_FLAG = 'Y'
            sess.commit()


    @staticmethod
    def flush_nodes(clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as sess:
            inactive_node_id_list = [node.NODE_ID for node in sess.query(Node)\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Node.NODE_ACTIVE_FLAG == 'N'
                    )
                )
            ]
            sess.query(Node)\
                .filter(Node.NODE_ID.in_(inactive_node_id_list))\
                .delete(synchronize_session=False)


    @staticmethod
    def purge_all_nodes(clus_name:str, repo_name:str) -> None:
        #TO DO: Write all AWS related Code here.
        with db_obj.session_scope() as sess:
            all_node_id_list = [node.NODE_ID for node in sess.query(Node)\
                .join(Cluster)\
                .join(Repository)\
                .filter(
                    and_(
                        Cluster.CLUSTER_NAME == clus_name.upper()
                        , Repository.REPO_NAME == repo_name.upper()
                        , Node.NODE_ACTIVE_FLAG == 'Y'
                        , Cluster.CLUSTER_ACTIVE_FLAG == 'Y'
                        , Repository.REPO_ACTIVE_FLAG == 'Y'
                    )
                )
            ]
            sess.query(Node)\
                .filter(Node.NODE_ID.in_(all_node_id_list))\
                .delete(synchronize_session=False)
