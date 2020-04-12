import json
from os.path import join, abspath, dirname

class Config():
    def __init__(self, configPath, Dir):
        self.configPath = configPath
        self.dir = Dir
        self.configDir = join(self.configPath, self.dir)

    @property
    def content(self):
        with open(self.configDir) as js:
            f = json.load(js)
            js.close()
        return f

    def set_config(self, data):
        with open(self.configDir, 'w') as js:
            json.dump(data, js)
            js.close()
