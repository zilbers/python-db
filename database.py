import json
from pathlib import Path
import os

PATH = "~\\Documents\\GitHub\\python-db\\"


class JsonDB(object):
    def __init__(self, location):
        self.location = os.path.expanduser(location)
        self.load(self.location)

    def load(self, location):
        if os.path.exists(location):
            self._load()
        else:
            self.db = {}
        return True

    def _load(self):
        self.db = json.load(open(self.location, "r"))

    def dumpdb(self):
        try:
            json.dump(self.db, open(self.location, "w+"))
            return True
        except:
            return False

    def save(self, key, value):
        try:
            self.db[str(key)] = value
            self.dumpdb()
            return True
        except Exception as e:
            print("[X] Error Saving Values to Database : " + str(e))
            return False

    def get(self, key):
        try:
            return self.db[key]
        except KeyError:
            print("No Value Can Be Found for " + str(key))
            return False

    def delete(self, key):
        if not key in self.db:
            return False
        del self.db[key]
        self.dumpdb()
        return True


class ColumnDB(object):
    def __init__(self, name, schema):
        # self.location = os.path.expanduser(PATH + name)
        self.name = name
        if os.path.exists("./%s" % (name,)):
            answer = input("Schema exists, overwrite? [Y/n]: ")
            if answer == 'n':
                print('Using old schema.')
                return
        Path("./%s" % (name,)).mkdir(parents=True, exist_ok=True)
        for row in schema:
            with open('./%s/%s.txt' % (self.name, row,), 'w') as f:
                f.write("")
        print('New schema created.')

    def save(self, key, value):
        path = './%s/%s.txt' % (self.name, key,)
        if os.path.exists(path):
            with open(path, 'a') as f:
                f.write(value + ',')
            return print('Added value to %s' % key)
        else:
            return print('Key does not exists')

    def get(self, key):
        path = './%s/%s.txt' % (self.name, key,)
        if os.path.exists(path):
            with open(path, 'r') as f:
                column_values = f.read()
                self.current = column_values.split(',')[0:-1]
                print(self.current)
            return
        else:
            return print('Key does not exists')
