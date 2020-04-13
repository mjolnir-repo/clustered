class Error(Exception):
    pass

class EncryptorError(Error):
    pass

class RepositoryError(Error):
    pass

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