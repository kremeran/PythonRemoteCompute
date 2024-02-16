from py_remote_compute.database.base_db import Database
from sys import platform as operating_system
import platform
from time import sleep 
from threading import Thread
from pathlib import Path
import os
import shutil


class ComputeServer():
    
    def __init__(self, database:Database, server_id:str=platform.node(), data_dir:str='.tmp'):
        self.available_functions = {}
        self.server_id = server_id
        self.db = database
        self.base_dir = os.getcwd()
        self.data_dir = self.base_dir + '/' + data_dir
        
    def start(self, block=True):
        self.admin_config = self.db.get_document('admin/config')
        
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
         
    def _got_new_job(self, doc_path:str, doc_id:str, event):
        job_obj = self.db.get_document(doc_path)

        # Create dir to run job
        job_path = self.data_dir + '/' + doc_id
        Path(job_path).mkdir(parents=True, exist_ok=True)
        os.chdir(job_path)
        if 'input_files' in job_obj:
            for file in job_obj['input_files']:
                self.db.fetch_file(doc_id+'/'+file['cloud'], job_path+'/'+file['cloud'])
            
        print(f'SERVER: Running Job [{doc_id}]')
        try:
            result = self.execute_function(job_obj['func_name'], job_obj['args'])
            job_obj['result'] = result
            job_obj['finished_time'] = self.db.timestamp_token()
            job_obj['compute_server'] = self.server_id
            job_obj['error'] = False
        except Exception as e:
            job_obj['result'] = 'SERVER: ' + str(e)
            job_obj['finished_time'] = self.db.timestamp_token()
            job_obj['compute_server'] = self.server_id
            job_obj['error'] = True
        
        if 'output_files' in job_obj:
            for file in job_obj['output_files']:
                self.db.store_file(file['cloud'], doc_id+'/'+file['cloud'])
                
        os.chdir(self.base_dir)
        try:
            shutil.rmtree(job_path)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
        
        self.db.set_document(f'results/{doc_id}', job_obj)
        self.db.delete_document(doc_path)
        
    def _create_job_queue_listener(self):
        self.db.on_document_added(f'servers/{self.server_id}/job_queue', self._got_new_job)
        
    def _start_heartbeat(self):
        Thread(daemon=True, target=self._run_heartbeat).start()
        
    def _run_heartbeat(self):
        while True:
            server_doc_path = f'servers/{self.server_id}'
            server_doc = self.db.get_document(server_doc_path)
            server_doc['heartbeat'] = self.db.timestamp_token()
            if 'os' not in server_doc:
                server_doc['os'] = operating_system
            if 'available_functions' not in server_doc:
                server_doc['available_functions'] = list(self.available_functions.keys())
            self.db.set_document(f'servers/{self.server_id}', server_doc)
            sleep(self.admin_config['heartbeat_time'])
        