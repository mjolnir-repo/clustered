"""
Declare environmetal variables like database details, user details etc
"""
import os
import json
import pathlib


class EnvironmetConfiguration:
    def __init__(self):
        pass
    
    @classmethod
    def _get_env_config(cls):
        app_path = os.environ.get('CLUSTERED_APP_PATH')
        env_file = app_path + '/clustered/env/ENV_Config.json'
        with open(env_file, 'r') as env_file_content:
            env_file_data = json.load(env_file_content)
        return env_file_data


class Environment:
	def __init__(self):
		self.env_config = EnvironmetConfiguration._get_env_config()
		self.AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
		self.AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
		self.AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', '')

	@property
	def DATABASE_URL(self):
		_db_list = self.env_config.get('DB', [])
		if _db_list:
			_active_db = {}
			for _db in _db_list:
				if _db.get('DB_ACTIVE_FLAG', 0):
					_active_db = _db
					break
			else:
				raise Exception("No Active database configuratio is available in " + os.environ.get('CLUSTERED_APP_PATH', '') + "/clusterd/env/ENV_Config.json")
		else:
			raise Exception("Database configuratoin is not present in " + os.environ.get('CLUSTERED_APP_PATH', '') + "/clusterd/env/ENV_Config.json. Please add the same or refer documentation.")
		if _active_db.get('DB_ENGINE', 'sqlite').lower() == 'sqlite':
			_db_file = _active_db.get('DB_DETAILS', {}).get('DB_FILE','clustered.db')
			return f'sqlite:///{_db_file}'

env = Environment()
