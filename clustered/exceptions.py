class Error(Exception):
    pass

class EncryptorError(Error):
    pass

class RepositoryError(Error):
    pass

class ClusterError(Error):
    pass

class NodeError(Error):
    pass

class UnavailableActionError(Error):
    def __init__(self, msg="Requested action is not available."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<UnavailableActionError({self.msg})>"

class WrongActionInvocationError(Error):
    def __init__(self, msg="Requested action can not be invoked in the current state."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<WrongActionInvocationError({self.msg})>"


###################################### Encryptor Engine Exceptions ######################################

class EncryptorNotPresentError(EncryptorError):
    def __init__(self, msg="Requested Encryptor is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<EncryptorNotPresentError({self.msg})>"


class EncryptorAlreadyExistsError(EncryptorError):
    def __init__(self, msg="Requested Encryptor already exists."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<EncryptorAlreadyExistsError({self.msg})>"


###################################### Repository Engine Exceptions ######################################


class RepositoryNotPresentError(RepositoryError):
    def __init__(self, msg="Requested Repository is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<RepositoryNotPresentError({self.msg})>"


class RepositoryAlreadyExistsError(RepositoryError):
    def __init__(self, msg="Requested Repository already exists."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<RepositoryAlreadyExistsError({self.msg})>"


###################################### Cluster Engine Exceptions ######################################


class ClusterNotPresentError(ClusterError):
    def __init__(self, msg="Requested Cluster is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ClusterNotPresentError({self.msg})>"


class ClusterAlreadyExistsError(ClusterError):
    def __init__(self, msg="Requested Cluster already exists."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ClusterAlreadyExistsError({self.msg})>"


###################################### Cluster Engine Exceptions ######################################


class NodeNotPresentError(NodeError):
    def __init__(self, msg="Requested Node is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<NodeNotPresentError({self.msg})>"


class NodeAlreadyExistsError(NodeError):
    def __init__(self, msg="Requested Node already exists."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<NodeAlreadyExistsError({self.msg})>"


class MasterNodeNotPresentError(NodeError):
    def __init__(self, msg="Master node not present for the parent Cluster already exists."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<MasterNodeNotPresentError({self.msg})>"


class MasterNodeAlreadyExistsError(NodeError):
    def __init__(self, msg="One master node already exists for the parent cluster. Only one master node is allowed per cluster."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ClusterAlreadyExistsError({self.msg})>"

