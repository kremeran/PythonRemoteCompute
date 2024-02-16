from py_remote_compute.client import Client
from py_remote_compute.database.base_db import Database
from py_remote_compute.database.firebase_db import FirebaseDB
from test_library import multiply_nth_primes, json_obj_to_values

fb = FirebaseDB('firebase-key.json', 'clipai-e1d66.appspot.com')
client = Client(fb, 'client')

input_files = [
     {
          'local': 'tests/obj.json',
          'cloud': 'c_obj.json'
     }
]

output_files = [
     {
          'cloud': 'c_vals.json',
          'local': 'tests/vals.json'
     }
]

# cloud
# clipai-e1d66.appspot.com/{job_id}/tesxt.mp4

# server
# .tmp/job_id/tesxt.mp4

result = client.run(json_obj_to_values, ['c_obj.json', 'c_vals.json'], input_files, output_files)
# result = client.run(multiply_nth_primes, [2000,2001])
print(result)
