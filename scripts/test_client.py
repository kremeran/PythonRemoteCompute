from context import Client
from test_library import multiply_nth_primes, json_obj_to_values

client = Client('firebase-key.json')

input_files = [
     {
          'local': 'scripts/obj.json',
          'cloud': 'c_obj.json'
     }
]

output_files = [
     {
          'cloud': 'c_vals.json',
          'local': 'vals.json'
     }
]

# cloud
# clipai-e1d66.appspot.com/{job_id}/tesxt.mp4

# server
# .tmp/job_id/tesxt.mp4

result = client.run(json_obj_to_values, ['c_obj.json', 'c_vals.json'], input_files, output_files)
# result = client.run(multiply_nth_primes, ['raw_audio.mp3', 8001], input_files, output_files)
print('result')
