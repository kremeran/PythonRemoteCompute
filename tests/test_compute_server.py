from py_remote_compute.compute_server import ComputeServer
from py_remote_compute.database.base_db import Database
from py_remote_compute.database.firebase_db import FirebaseDB
from test_library import multiply_nth_primes, json_obj_to_values

fb:Database = FirebaseDB('firebase-key.json', 'clipai-e1d66.appspot.com')
server = ComputeServer(fb)
server.load_function(multiply_nth_primes)
server.load_function(json_obj_to_values)
server.start()
