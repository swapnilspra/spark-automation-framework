import os

ENV_VARIABLE_PREFIX = 'TEAMS_HOOK_'
class EnvUrlLoader:
    def load(self):
        return EnvUrlLoader.extract(os.environ, ENV_VARIABLE_PREFIX)
    
    @staticmethod
    def extract(src, prefix):
        return list({value for key, value in src.items() if key.startswith(ENV_VARIABLE_PREFIX) })
