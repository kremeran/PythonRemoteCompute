from py_remote_compute.admin_server import AdminServer
from py_remote_compute.database.base_db import Database
from py_remote_compute.database.firebase_db import FirebaseDB

fb:Database = FirebaseDB('firebase-key.json', 'clipai-e1d66.appspot.com')
admin_config = {
    'heartbeat_time': 10,
}
admin_server = AdminServer(fb, admin_config)
admin_server.start()