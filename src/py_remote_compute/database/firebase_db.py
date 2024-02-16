from py_remote_compute.database.base_db import Database, CollectionListener

from types import FunctionType
from platform import node
from threading import Event

from firebase_admin import firestore, credentials, initialize_app, storage
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.watch import Watch, DocumentChange

class FirebaseDB(Database):

    def __init__(self, firebase_key_path:str, storage_url:str, client_name:str=node()):

        self.app = initialize_app(credentials.Certificate(firebase_key_path), {
            'storageBucket': storage_url
        }, name = client_name)

        self.bucket = storage.bucket(app=self.app)
        self.client = firestore.client(app=self.app)

    def timestamp_token(self) -> str:
        return firestore.firestore.SERVER_TIMESTAMP

    def get_document(self, doc_path:str) -> dict:
        doc_obj = self.client.document(doc_path).get().to_dict()
        if doc_obj is None:
            doc_obj = {}
        return doc_obj

    def create_document(self, col_path:str, doc_obj:dict) -> str: 
        doc_ref = self.client.collection(col_path).document()
        doc_ref.set(doc_obj)
        return doc_ref.id
    
    def set_document(self, doc_path: str, doc_obj: dict) -> None:
        doc_ref = self.client.document(doc_path)
        doc_ref.set(doc_obj)
    
    def move_document(self, from_doc_path:str, to_doc_path:str) -> bool:
        from_doc_ref = self.client.document(from_doc_path)
        if len(list(from_doc_ref.collections())) > 0:
            raise Exception('Cant move document with subcollection')
        from_doc_obj = from_doc_ref.get().to_dict()
        if from_doc_obj is not None:
            to_doc_ref = self.client.document(to_doc_path)
            to_doc_ref.set(from_doc_obj)
            from_doc_ref.delete()
            return True
        return False

    def get_collection(self, col_path:str) -> dict:
        to_return = {}
        doc_ref:DocumentReference
        doc_refs = self.client.collection(col_path).list_documents()
        for doc_ref in doc_refs:
            to_return[doc_ref.id] = doc_ref.get().to_dict()
        return to_return
    
    def store_file(self, local_path: str, cloud_path: str) -> None:
        self.bucket.blob(cloud_path).upload_from_filename(local_path)
    
    def fetch_file(self, cloud_path: str, local_path: str) -> None:
        self.bucket.blob(cloud_path).download_to_filename(local_path)

    def delete_file(self, cloud_path: str) -> None:
        return self.bucket.blob(cloud_path).delete()
    
    def _on_document(self, coll_path:str, func:FunctionType, action:str) -> CollectionListener:
        event = Event()

        def got_change(doc_snapshot:list[DocumentSnapshot], changes:list[DocumentChange], read_time):
            for change in changes:
                if change.type.name == action:
                    doc_path = f'{coll_path}/{change.document.id}'
                    func(doc_path, change.document.id, event)

        col_ref = self.client.collection(coll_path)
        col_watch = col_ref.on_snapshot(got_change)

        return CollectionListener(col_watch.unsubscribe, event)

    def on_document_added(self, coll_path:str, func:FunctionType) -> CollectionListener:
        return self._on_document(coll_path, func, 'ADDED')
    
    def on_document_removed(self, coll_path:str, func:FunctionType) -> CollectionListener:
        return self._on_document(coll_path, func, 'REMOVED')
    
    def on_document_modified(self, coll_path:str, func:FunctionType) -> CollectionListener:
        return self._on_document(coll_path, func, 'MODIFIED')

    def delete_document(self, doc_path:str) -> bool:
        self.client.document(doc_path).delete()
        return True
    
    def delete_collection(self, col_path:str) -> int:
        docs_removed = 0
        doc_refs = self.client.collection(col_path).list_documents()
        doc_ref:DocumentReference
        for doc_ref in doc_refs:
            doc_path = f'{col_path}/{doc_ref.id}'
            col_ref:CollectionReference
            for col_ref in doc_ref.collections():
                docs_removed += self.delete_collection(f'{doc_path}/{col_ref.id}')
            doc_ref.delete()
            docs_removed += 1
        return docs_removed