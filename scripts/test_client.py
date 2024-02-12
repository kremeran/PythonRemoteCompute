from context import Client
from test_library import multiply_nth_primes

client = Client('firebase-key.json')

result = client.run(multiply_nth_primes, [8000, 8001])

print(result)
