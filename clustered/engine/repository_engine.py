"""
All repository related tasks will be defined here.
These tasks will be independent of any object which are not present in this module. 
"""

from ..env import env


class RepositoryEngine:
    def __init__(self):
        pass

    def get_repositories(self):
        print(env.DATABASE_URL)