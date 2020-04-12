import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import UniqueConstraint


Base = declarative_base()
class Repository(Base):
    __tablename__ = 'REPOSITORY'

    REPO_ID = Column(Integer, primary_key=True)
    REPO_NAME = Column(String(100), nullable=False)
    REPO_DESC = Column(String(1000), nullable=True)
    REPO_ACCESS_KEY_ENCRYPTED = Column(String(50), nullable=False)
    REPO_SECRET_KEY_ENCRYPTED = Column(String(50), nullable=False)
    REPO_REGION = Column(String(20), nullable=False)
    REPO_VPC_ID = Column(String(50), nullable=True)
    REPO_SUBNET_LIST = Column(String(100), nullable=True)
    REPO_STATE = Column(String(10), nullable=False)
    REPO_ACTIVE_FLAG = Column(String(1), nullable=False)

    CLUSTERS = relationship("Cluster", back_populates="REPOSITORY")

    UniqueConstraint('REPO_NAME', name = 'repo_name_unique')

    def __repr__(self):
        return f"""<Repository(
            REPO_NAME = {self.REPO_NAME},
            REPO_DESC = {self.REPO_DESC},
            REPO_ACCESS_KEY_ENCRYPTED = {self.REPO_ACCESS_KEY_ENCRYPTED},
            REPO_SECRET_KEY_ENCRYPTED = {self.REPO_SECRET_KEY_ENCRYPTED},
            REPO_SECRET_KEY_ENCRYPTED = {self.REPO_REGION},
            REPO_VPC_ID = {self.REPO_VPC_ID},
            REPO_SUBNET_LIST = {self.REPO_SUBNET_LIST},
            REPO_STATE = {self.REPO_STATE},
            REPO_ACTIVE_FLAG = {self.REPO_ACTIVE_FLAG}
        )>"""


class Cluster(Base):
    __tablename__ = 'CLUSTER'

    CLUSTER_ID = Column(Integer, primary_key=True)
    CLUSTER_REPO_ID = Column(Integer, ForeignKey('REPOSITORY.REPO_ID'))
    CLUSTER_NAME = Column(String(100), nullable=False)
    CLUSTER_DESC = Column(String(1000), nullable=True)
    CLUSTER_SECURITY_GROUP_ID = Column(String(50), nullable=True)
    CLUSTER_WHITELISTED_IP_SET = Column(String(5000), nullable=True)
    CLUSTER_STATE = Column(String(10), nullable=False)
    CLUSTER_ACTIVE_FLAG = Column(String(1), nullable=False)

    REPOSITORY = relationship("Repository", back_populates="CLUSTERS")
    NODES = relationship("Node", back_populates="CLUSTER")

    UniqueConstraint('CLUSTER_REPO_ID', 'CLUSTER_NAME', name = 'cluster_name_unique')

    def __repr__(self):
        return f"""<Cluster(
            CLUSTER_REPO_ID = {self.CLUSTER_REPO_ID},
            CLUSTER_NAME = {self.CLUSTER_NAME},
            CLUSTER_DESC = {self.CLUSTER_DESC},
            CLUSTER_SECURITY_GROUP_ID = {self.CLUSTER_SECURITY_GROUP_ID}, 
            CLUSTER_WHITELISTED_IP_SET = {self.CLUSTER_WHITELISTED_IP_SET},
            CLUSTER_ACTIVE_FLAG = {self.CLUSTER_ACTIVE_FLAG}
        )>"""


class Node(Base):
    __tablename__ = 'NODE'

    NODE_ID = Column(Integer, primary_key=True)
    NODE_CLUSTER_ID = Column(Integer, ForeignKey('CLUSTER.CLUSTER_ID'))
    NODE_NAME = Column(String(100), nullable=False)
    NODE_DESC = Column(String(1000), nullable=True)
    NODE_TYPE = Column(String(1), nullable=False)
    NODE_INSTANCE_TYPE = Column(String(10), nullable=False)
    NODE_INSTANCE_ID = Column(String(50), nullable=True)
    NODE_KEY_PAIR_NAME = Column(String(100), nullable=False)
    NODE_BLOCK_DEVICE_MAPPING = Column(String(100), nullable=False)
    NODE_STATE = Column(String(10), nullable=False)
    NODE_ACTIVE_FLAG = Column(String(1), nullable=False)

    CLUSTER = relationship("Cluster", back_populates="NODES")

    UniqueConstraint('NODE_CLUSTER_ID', 'NODE_NAME', name = 'node_name_unique')

    def __repr__(self):
        return f"""<Node(
            NODE_CLUSTER_ID = {self.NODE_CLUSTER_ID},
            NODE_NAME = {self.NODE_NAME},
            NODE_DESC = {self.NODE_DESC},
            NODE_TYPE = {self.NODE_TYPE},
            NODE_INSTANCE_TYPE = {self.NODE_INSTANCE_TYPE}, 
            NODE_INSTANCE_ID = {self.NODE_INSTANCE_ID},
            NODE_KEY_PAIR_NAME = {self.NODE_KEY_PAIR_NAME},
            NODE_BLOCK_DEVICE_MAPPING = {self.NODE_BLOCK_DEVICE_MAPPING},
            NODE_STATE = {self.NODE_STATE},
            CLUSTER_ACTIVE_FLAG = {self.NODE_ACTIVE_FLAG}
        )>"""
