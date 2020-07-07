class Error(Exception):
    pass


class ConfigurationError(Error):
    pass


class EncryptorError(Error):
    pass


class RepositoryError(Error):
    pass


class ClusterError(Error):
    pass


class NodeError(Error):
    pass


class UnexpectedSystemError(Error):
    def __init__(self, msg="Something gone wrong un-expectedly. Contact Administrator."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<UnexpectedSystemError({self.msg})>"


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


###################################### Application Engine Exceptions ######################################


class ApplicationAlreadyInitiatedError(ConfigurationError):
    def __init__(self, msg="Clustered application is already initiated for this user. To make any changes to existing configuration run `clustered refresh`."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ApplicationAlreadyInitiatedError({self.msg})>"


class ApplicationNotInitiatedError(ConfigurationError):
    def __init__(self, msg="Clustered application is not initiated yet for this user. To initiate execute `clustered init`."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ApplicationNotInitiatedError({self.msg})>"


class ApplicationWorkspaceBuildingError(ConfigurationError):
    def __init__(self, msg="Clustered application workspace could not be built succesfully."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ApplicationWorkspaceBuildingError({self.msg})>"


class ApplicationDatabaseSetupError(ConfigurationError):
    def __init__(self, msg="Application meta-database could not be initiated properly."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ApplicationDatabaseSetupError({self.msg})>"


class ApplicationDatabaseCleanupError(ConfigurationError):
    def __init__(self, msg="Application meta-database is not purged properly."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ApplicationDatabaseCleanupError({self.msg})>"


class ApplicationDatabaseUnsupportedError(ConfigurationError):
    def __init__(self, msg="Provided database type is not yet supported to be clustered meta-database."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ApplicationDatabaseUnsupportedError({self.msg})>"


###################################### Configuration Engine Exceptions ######################################


class ConfigurationFileNotAvailableError(ConfigurationError):
    def __init__(self, msg="Configuration file is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ConfigurationFileNotAvailableError({self.msg})>"


class ConfigurationNotAvailableError(ConfigurationError):
    def __init__(self, msg="Requested Configuration is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ConfigurationNotAvailableError({self.msg})>"


class ConfigurationInitiationError(ConfigurationError):
    def __init__(self, msg="Configuration initiation process has failed."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ConfigurationInitiationError({self.msg})>"


class ConfigurationExtractionError(ConfigurationError):
    def __init__(self, msg="Configuration extraction process has failed."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<ConfigurationExtractionError({self.msg})>"


###################################### Encryptor Engine Exceptions ######################################


class EncryptorNotPresentError(EncryptorError):
    def __init__(self, msg="Requested Encryptor is not found."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<EncryptorNotPresentError({self.msg})>"


class EncryptorNotActiveError(EncryptorError):
    def __init__(self, msg="Requested Encryptor is not active."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<EncryptorNotActiveError({self.msg})>"


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


class RepositoryNotActiveError(RepositoryError):
    def __init__(self, msg="Requested Repository is not Active."):
        self.msg = msg

    def __str__(self):
        return self.msg
    
    def __repr__(self):
        return f"<RepositoryNotActiveError({self.msg})>"


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

