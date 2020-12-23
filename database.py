import json
from json.decoder import JSONDecodeError
from pathlib import Path
import os

PATH = "~\\Documents\\GitHub\\python-db\\"


class Json(object):
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


class Column(object):
    def __init__(self, name, schema=[]):
        self.name = name
        self.data = {}
        self.row_data = None
        self.length = 0

        if os.path.exists("./%s" % (name,)):
            answer = input("Database exists, overwrite? [Y/n]: ")
            if answer == 'n':
                with open('./meta/%s.txt' % (name,)) as f:
                    self.schema = json.load(f)
                for key in self.schema:
                    with open('./%s/%s.txt' % (self.name, key['key'],), 'r') as f:
                        try:
                            column_values = json.load(f)
                            self.length = len(column_values)
                            self.data[key['key']] = column_values
                        except JSONDecodeError:
                            self.length = 0
                            self.data[key['key']] = []
                print('Using old database.')
                return
        if len(schema) == 0:
            print('Not a valid schema.')
            return

        else:
            self.schema = schema
            if not os.path.exists("./meta"):
                Path("./meta").mkdir(parents=True, exist_ok=True)

            with open('./meta/%s.txt' % (self.name,), 'w', encoding='utf-8') as f:
                json.dump(schema, f, ensure_ascii=False, indent=4)

            Path("./%s" % (self.name,)).mkdir(parents=True, exist_ok=True)
            for key in schema:
                with open('./%s/%s.txt' % (self.name, key['key'],), 'w') as f:
                    f.write("")
                self.data[key['key']] = []
            print('New database created.')
            return

    def add(self, values):
        if len(self.schema) != len(values):
            print('Wrong values, this is the schema:')
            return self.schema

        for index in range(len(self.schema)):
            schema_row = self.schema[index]
            value = values[index]
            if self._type(schema_row['type'], value):
                self.data[schema_row['key']].append(value)
            else:
                print('Wrong values, this is the schema:')
                return self.schema
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
            if id != None and id <= self.length - 1:
                return self.row_data[id]

        if id != None and id < self.length - 1:
            row = []
            for key in self.schema:
                row.append(self.data[key['key']][id])
            return row
        else:
            data = []
            for index in range(self.length):
                row = {}
                for key in self.schema:
                    row[key['key']] = self.data[key['key']][index]
                data.append(row)
            self.row_data = data
            print("This is the data:")
            return data

    def save(self):
        Path("./%s" % (self.name,)).mkdir(parents=True, exist_ok=True)
        for key in self.schema:
            with open('./%s/%s.txt' % (self.name, key['key'],), 'w', encoding='utf-8') as f:
                json.dump(self.data[key['key']], f,
                          ensure_ascii=False, indent=4)
        print('Saved database values.')
        return

    def count(self, key, value):
        return self.data[key].count(value)

    def _type(self, schema_type, value):
        if schema_type == 'str' or schema_type == 'string':
            return type(value) is str
        if schema_type == 'int' or schema_type == 'integer':
            return type(value) is int
        else:
            return False


class Binary(object):
    def __init__(self, name, schema=[]):
        self.name = name
        self.data = {}
        self.length = 0
        # Schema: block - 255
        # Input: int
        # Index: Json, key - value, value - array of locations in file (id) * 255
        if os.path.exists("./%s" % (name,)):
            answer = input("Database exists, overwrite? [Y/n]: ")
            if answer == 'n':
                with open('./meta/%s.txt' % (name,)) as f:
                    self.schema = json.load(f)
                print('Using old database.')
                return
        if len(schema) == 0:
            print('Not a valid schema.')
            return

        self.schema = schema
        if not os.path.exists("./meta"):
            Path("./meta").mkdir(parents=True, exist_ok=True)

        with open('./meta/%s.txt' % (self.name,), 'w', encoding='utf-8') as f:
            json.dump(schema, f, ensure_ascii=False, indent=4)

        Path("./%s" % (self.name,)).mkdir(parents=True, exist_ok=True)
        for key in schema:
            with open('./%s/%s.txt' % (self.name, key['key'],), 'w') as f:
                f.write("")
            self.data[key['key']] = []
        print('New database created.')
        return

    def add(self, values):
        for index in range(len(values)):
            if isinstance(values[index], int) and values[index] <= 255:
                with open('./%s/%s.txt' % (self.name, self.schema[index]['key'],), 'a') as f:
                    f.write(f'{values[index]:08b}')
                self._index(values[index], self.length)
                self.length += 1
                message = 'Added values.'
            else:
                message = 'POC supports only 8bit int.'
        return print(message)

    def _index(self, key, location):
        path = './meta/%s/index/%s.txt' % (self.name, key)

        if not os.path.exists("./meta/%s" % (self.name)):
            Path("./meta/%s" % (self.name)).mkdir(parents=True, exist_ok=True)

        if not os.path.exists("./meta/%s/index" % (self.name)):
            Path("./meta/%s/index" % (self.name)
                 ).mkdir(parents=True, exist_ok=True)

        if os.path.exists(path):
            append_write = 'a'
        else:
            append_write = 'w'
        with open(path, append_write) as f:
            f.write(f'{location:08b}')
