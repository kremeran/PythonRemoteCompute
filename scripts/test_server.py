from remote_compute.Server import Server

s = Server('firebase-key.json', 'clipai-e1d66.appspot.com')
s.listen_for_jobs()
