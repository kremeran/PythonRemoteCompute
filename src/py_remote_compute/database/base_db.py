import abc
from types import FunctionType
from threading import Event

class CollectionListener:
    def __init__(self, unsubscribe_func:FunctionType, event:Event):
        self.event = event
        self.unsubscribe_func = unsubscribe_func

    def unsubscribe(self):
        self.unsubscribe_func()

    def wait(self):
        if self.event is not None:
            self.event.wait()
        else:
            raise Exception('Attempting to wait on CollectionListener with no event set.')

    def set(self):
        if self.event is not None:
            self.event.set()
        else:
            raise Exception('Attempting to set CollectionListener with no event set.')

class Database(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def timestamp_token(self) -> str:
        pass

    @abc.abstractmethod 
    def get_document(self, doc_path:str) -> dict: 
        pass

    @abc.abstractmethod 
    def create_document(self, col_path:str, doc_obj:dict) -> str: 
        pass

    @abc.abstractmethod
    def set_document(self, doc_path:str, doc_obj:dict) -> None:
        pass

    @abc.abstractmethod
    def move_document(self, from_doc_path:str, to_doc_path:str) -> bool:
        pass

    @abc.abstractmethod
    def get_collection(self, col_path:str) -> dict:
        pass

    @abc.abstractmethod
    def fetch_file(self, cloud_path:str, local_path:str) -> None:
        pass

    @abc.abstractmethod
    def store_file(self, local_path:str, cloud_path:str) -> None:
        pass

    @abc.abstractmethod
    def delete_file(self, cloud_path:str) -> None:
        pass

    @abc.abstractmethod 
    def on_document_added(self, coll_path:str, func:FunctionType) -> CollectionListener: 
        pass

    @abc.abstractmethod 
    def on_document_removed(self, coll_path:str, func:FunctionType) -> CollectionListener: 
        pass

    @abc.abstractmethod 
    def on_document_modified(self, coll_path:str, func:FunctionType) -> CollectionListener: 
        pass

    @abc.abstractmethod
    def delete_document(self, doc_path:str) -> bool:
        pass

    @abc.abstractmethod 
    def delete_collection(self, coll_path:str) -> int:
        pass

    def clean_all(self) -> int:
        removed_documents = 0
        removed_documents += self.delete_collection('results')
        removed_documents += self.delete_collection('job_staging')
        for server_id in self.get_collection('servers'):
            removed_documents += self.delete_collection(f'servers/{server_id}')
        removed_documents += self.delete_collection('servers')
        removed_documents += self.delete_collection('admin')
