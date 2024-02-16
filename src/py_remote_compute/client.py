from google.cloud.firestore_v1.base_query import FieldFilter
from py_remote_compute.database.base_db import Database
from time import sleep
from datetime import datetime
from threading import Event
import platform
import os
from pprint import pformat
from textwrap import indent

class Client():
    
    def __init__(self, database:Database, client_id:str):
        self.db = database
        self.client_id = client_id

    def run(self, func_ref, args, input_files={}, output_files={}):
        job = {
            'func_name': func_ref.__name__,
            'args': args,
            'request_time': self.db.timestamp_token(),
            'client_id': self.client_id,
            'input_files': input_files,
            'output_files': output_files,
        }
 
        job_id = self.db.create_document('job_staging', job)
        
        for file in input_files:
            self.db.store_file(file['local'], job_id + '/' + file['cloud'])

        def response_ready(doc_path:str, doc_id:str, event:Event):
            if doc_id == job_id:
                event.set()
        
        listener = self.db.on_document_added('results', response_ready)
        listener.wait()
        listener.unsubscribe()
        
        response_obj = self.db.get_document(f'results/{job_id}')
        result = response_obj['result']
        if response_obj['error']:
            print(' '+('_'*79))
            print('|')
            print(f'| CLIENT: job failed with error: [{result}]')
            print('|'+('_'*79))
            print('|')
            print(indent(pformat(response_obj), '| '))
            print('|'+('_'*79))
            self.db.delete_document(f'results/{job_id}')
            return False
        else:
            for file in output_files:
                self.db.fetch_file(job_id + '/' + file['cloud'], file['local'])
            
            for file in input_files:
                self.db.delete_file(job_id + '/' + file['cloud'])

            for file in output_files:
                self.db.delete_file(job_id + '/' + file['cloud'])
            
            compute_time = (response_obj['finished_time'] - response_obj['request_time']).total_seconds()
            compute_server = response_obj['compute_server']
            
            self.db.delete_document(f'results/{job_id}')
            print(f'CLIENT: job [{job_id}] run on [{compute_server}] in {compute_time} seconds.')
            return result
