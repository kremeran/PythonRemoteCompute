from remote_compute.utils import init_firebase, get_admin_config
from sys import platform as operating_system
import platform
from time import sleep 
from threading import Thread
from pathlib import Path
import os
import sys
import shutil


class Server():
    
    def __init__(self, firebase_key_path, server_id=platform.node(), data_dir='.tmp'):
        self.firebase_key_path = firebase_key_path
        self.server_id = server_id
        self.available_functions = {}
        self.base_dir = os.getcwd()
        self.data_dir = self.base_dir + '/' + data_dir
        
    def start(self, block=True):
        self.admin_config = get_admin_config(self.server_id, self.firebase_key_path)
        self.fb = init_firebase(self.admin_config, self.server_id, self.firebase_key_path)
        
        self._create_server_doc()
        self._start_heartbeat()
        self._create_job_queue_listener()
        
        print('Starting compute server...')
        if block:
            while True:
                sleep(0.1)
        
    def execute_function(self, func_name, args):
        if func_name in self.available_functions:
            arg_tuple = tuple(args)       
            return self.available_functions[func_name](*arg_tuple)
        
    def load_function(self, func):
        if func.__name__ not in self.available_functions:
            self.available_functions[func.__name__] = func
        else:
            raise Exception('Function already added to server')
        
    def _fetch_file(self, job_id, cloud_name):
        blob = self.fb.bucket.blob(job_id + '/' + cloud_name)
        blob.download_to_filename(cloud_name)
        
    def _store_file(self, job_id, cloud_name):
        blob = self.fb.bucket.blob(job_id + '/' + cloud_name)
        blob.upload_from_filename(cloud_name)
         
    def _on_job_queue_snapshot(self):
        def on_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'REMOVED':
                    continue
                elif change.type.name == 'ADDED':
                    job_id = change.document.id
                    job_ref = self.fb.db.document(f'servers/{self.server_id}/job_queue/{job_id}')
                    job_dict = job_ref.get().to_dict()
                    
                    # Create dir to run job
                    job_path = self.data_dir + '/' + job_id
                    Path(job_path).mkdir(parents=True, exist_ok=True)
                    os.chdir(job_path)
                    if 'input_files' in job_dict:
                        for cloud_name in job_dict['input_files']:
                            self._fetch_file(job_id, cloud_name)
                        
                    print(f'SERVER: Running Job [{change.document.id}]')
                    try:
                        result = self.execute_function(job_dict['func_name'], job_dict['args'])
                        job_dict['result'] = result
                        job_dict['finished_time'] = self.fb.fs.SERVER_TIMESTAMP
                        job_dict['compute_server'] = self.server_id
                        job_dict['error'] = False
                    except Exception as e:
                        job_dict['result'] = 'SERVER: ' + str(e)
                        job_dict['finished_time'] = self.fb.fs.SERVER_TIMESTAMP
                        job_dict['compute_server'] = self.server_id
                        job_dict['error'] = True
                    
                    if 'output_files' in job_dict:
                        for cloud_name in job_dict['output_files']:
                            self._store_file(job_id, cloud_name)
                            
                    os.chdir(self.base_dir)
                    try:
                        shutil.rmtree(job_path)
                    except OSError as e:
                        print("Error: %s - %s." % (e.filename, e.strerror))
                    
                    result_ref = self.fb.db.document(f'results/{change.document.id}')
                    result_ref.set(job_dict)
                    
                    job_ref.delete()
        return on_snapshot
        
    def _create_job_queue_listener(self):
        # Create a callback on_snapshot function to capture changes
        job_queue_query = self.fb.db.collection(f'servers/{self.server_id}/job_queue')
        job_queue_query.on_snapshot(self._on_job_queue_snapshot())
        
    def _create_server_doc(self):
        doc_ref = self.fb.db.collection('servers').document(self.server_id)
        doc_ref.set({
            'os': operating_system,
            'available_functions': list(self.available_functions.keys()),
        })
        
    def _start_heartbeat(self):
        Thread(daemon=True, target=self._run_heartbeat).start()
        
    def _run_heartbeat(self):
        while True:
            server_ref = self.fb.db.document(f'servers/{self.server_id}')
            try:
                server_obj = server_ref.get().to_dict()
                server_obj['heartbeat'] = self.fb.fs.SERVER_TIMESTAMP
                server_ref.set(server_obj)
            except:
                print('Admin server probably deleted server doc. Creating new doc.')
                new_server_obj = {
                    'os': operating_system,
                    'available_functions': list(self.available_functions.keys()),
                    'heartbeat': self.fb.fs.SERVER_TIMESTAMP
                }
                server_ref.set(new_server_obj)
            sleep(self.admin_config['heartbeat_time'])
        