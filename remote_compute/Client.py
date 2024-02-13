from remote_compute.utils import init_firebase, get_admin_config
from google.cloud.firestore_v1.base_query import FieldFilter
from time import sleep
from datetime import datetime
import threading
import platform
import os
from pprint import pformat
from textwrap import indent

class Client():
    
    def __init__(self, firebase_key_path, client_id=platform.node()+'_client'):
        self.firebase_key_path = firebase_key_path
        self.client_id = client_id
        self.admin_config = get_admin_config(self.client_id, self.firebase_key_path)
        self.fb = init_firebase(self.admin_config, self.client_id, self.firebase_key_path)

    def run(self, func_ref, args, input_files=None, output_files=None):
        job = {
            'func_name': func_ref.__name__,
            'args': args,
            'request_time': self.fb.fs.SERVER_TIMESTAMP,
            'client_id': self.client_id,
        }
        job_ref = self.fb.db.collection('job_staging').document()
        job_id = job_ref.id
        
        if input_files is not None:
            f = []
            for file in input_files:
                self._store_file(file['local'], f'{job_id}/{file["cloud"]}')
                f.append(file["cloud"])
            job['input_files'] = f
        if output_files is not None:
            f = []
            for file in output_files:
                f.append(file['cloud'])
            job['output_files'] = f
        
        job_ref.set(job)
        
        response_ready = self._create_response_listener(job_id)
        response_ready.wait()
        
        response_ref = self.fb.db.document(f'results/{job_id}')
        response_obj = response_ref.get().to_dict()
        result = response_obj['result']
        if response_obj['error']:
            print(' '+('_'*79))
            print('|')
            print(f'| CLIENT: job failed with error: [{result}]')
            print('|'+('_'*79))
            print('|')
            print(indent(pformat(response_obj), '| '))
            print('|'+('_'*79))
            response_ref.delete()
            return False
        else:
            if output_files is not None:
                for file in output_files:
                    self._fetch_file(job_id, file['cloud'], file['local'])
            
            if input_files is not None or output_files is not None:
                self._delete_cloud_job_dir(job_id)
            
            compute_time = (response_obj['finished_time'] - response_obj['request_time']).total_seconds()
            compute_server = response_obj['compute_server']
            
            response_ref.delete()
            print(f'CLIENT: job [{job_id}] run on [{compute_server}] in {compute_time} seconds.')
            return result

    def _store_file(self, local_path, cloud_path):
        print(os.getcwd())
        blob = self.fb.bucket.blob(cloud_path)
        blob.upload_from_filename(local_path)
        
    def _fetch_file(self, job_id, cloud_name, local_path):
        blob = self.fb.bucket.blob(job_id + '/' + cloud_name)
        blob.download_to_filename(local_path)
        
    def _delete_cloud_job_dir(self, job_id):
        # TODO
        pass
        
    def _create_response_listener(self, job_id):
        response_ready = threading.Event()

        # Create a callback on_snapshot function to capture changes
        def got_new_response(doc_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'ADDED':
                    if change.document.id == job_id:
                        response_ready.set()

        results_ref = self.fb.db.collection('results')

        # Watch the document
        results_ref.on_snapshot(got_new_response)
        
        return response_ready
