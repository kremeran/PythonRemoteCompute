from context import Client
import threading
from time import sleep
from test_library import multiply_nth_primes

client = Client('firebase-key.json')

def run_job(num):
    client.run(multiply_nth_primes, [5000+(num), 5000+(num*2)])

threads = []

for x in range(1,11):
    t = threading.Thread(target=run_job, args=(x,))
    threads.append(t)

for t in threads:
    sleep(0.2)
    t.start()
    
print('All jobs requested. Waiting on responses.')
    
for t in threads:
    t.join()

print("Done!")
