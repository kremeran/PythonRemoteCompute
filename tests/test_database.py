from threading import Event
from py_remote_compute.database.firebase_db import FirebaseDB as Database
from time import sleep

fb = Database('firebase-key.json', 'clipai-e1d66.appspot.com')
fb.delete_collection('test')
fb.delete_collection('moved')
fb.set_document('test/doc', {'hello': 'world'})

def print_doc(doc_path:str, doc_id:str, event:Event):
    print(f'Document [{doc_id}] moved!')

listener = fb.on_document_added('moved', print_doc)
sleep(0.5)
fb.move_document('test/doc', 'moved/doc')


print('done!')