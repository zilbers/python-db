import database
import os

x = database.Binary('test', [{'key': 'age', 'type': 'integer'}, {
                    'key': 'money', 'type': 'integer'}])

x.add([1, 2])
x.add([100, 221])
