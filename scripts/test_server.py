from context import Server, Admin
from test_library import multiply_nth_primes, json_obj_to_values

admin_config = {
    'file_support': True,
    'folder_path': 'clipai-e1d66.appspot.com',
    'heartbeat_time': 10,
}

admin = Admin(admin_config, 'firebase-key.json')
admin.clean_firebase()
admin.start(block=False)

server = Server('firebase-key.json')
server.load_function(multiply_nth_primes)
server.load_function(json_obj_to_values)
server.start()
