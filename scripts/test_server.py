from context import Server, Admin
from test_library import multiply_nth_primes

admin_config = {
    'file_support': False,
    'folder_path': 'clipai-e1d66.appspot.com',
    'heartbeat_time': 10,
}

admin = Admin(admin_config, 'firebase-key.json')
admin.clean_job_staging()
admin.clean_results()
admin.start(block=False)

server = Server('firebase-key.json')
server.load_function(multiply_nth_primes)
server.start()
