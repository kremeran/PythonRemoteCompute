from context import Server, Admin
from test_library import multiply_nth_primes
from threading import Thread
from time import sleep

admin_config = {
    'file_support': False,
    'folder_path': 'clipai-e1d66.appspot.com',
    'heartbeat_time': 10,
}

def spawn_server(id):
    server = Server('firebase-key.json', server_id=f'server_{id}')
    server.load_function(multiply_nth_primes)
    server.start(block=False)

threads = []

for x in range(0,5):
    t = Thread(daemon=True, target=spawn_server, args=(x,))
    threads.append(t)

for t in threads:
    t.start()
    
while True:
    sleep(0.1)