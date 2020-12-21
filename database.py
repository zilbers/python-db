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
    def __init__(self, name, schema=[]):
        self.name = name
        self.data = {}
        self.row_data = None

        if os.path.exists("./%s" % (name,)):
            answer = input("Database exists, overwrite? [Y/n]: ")
            if answer == 'n':
                with open('./meta/%s.txt' % (name,), 'r') as f:
                    schema_values = f.read()
                    self.schema = schema_values.split(',')[0:-1]
                for key in self.schema:
                    with open('./%s/%s.txt' % (self.name, key,), 'r') as f:
                        column_values = f.read()
                        values = column_values.split(',')[0:-1]
                        self.length = len(values)
                        self.data[key] = values
                print('Using old database.')
                return

        if len(schema) == 0:
            print('Not a valid schema.')
            return
        else:
            self.schema = schema
            if not os.path.exists("./meta"):
                Path("./meta").mkdir(parents=True, exist_ok=True)
            with open('./meta/%s.txt' % (self.name,), 'w') as f:
                f.write(','.join(schema))

            Path("./%s" % (self.name,)).mkdir(parents=True, exist_ok=True)
            for key in schema:
                with open('./%s/%s.txt' % (self.name, key,), 'w') as f:
                    f.write("")
                self.data[key] = []
            print('New schema created.')
            return

    def add(self, values):
        if len(self.schema) != len(values):
            print('Wrong values, this is the schema:')
            return self.schema

        for index in range(len(self.schema)):
            self.data[self.schema[index]].append(values[index])
        self.length += 1
        print('Added this values:')
        return values

    def get_column(self, key, id=None):
        if key in self.data:
            if id != None:
                if id <= len(self.data[key]):
                    return self.data[key][id]
                else:
                    print('id does not exists, largest id is: %s' %
                          len(self.data[key]))
            return self.data[key]
        else:
            return print('Key does not exists.')

    def get(self, id=None):
        if self.row_data != None:
            if id != None and id <= self.length:
                return self.row_data[id]

        if id != None and id < self.length:
            row = []
            for key in self.schema:
                row.append(self.data[key][id])
            return row
        else:
            data = []
            for index in range(self.length):
                row = {}
                for key in self.schema:
                    row[key] = self.data[key][index]
                data.append(row)
            self.row_data = data
            print("This is the data:")
            return data
