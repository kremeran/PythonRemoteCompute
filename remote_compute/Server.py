from firebase_admin import firestore, credentials, initialize_app, storage
from os import getcwd
from pathlib import Path
from threading import Event
from sys import platform as operating_system
from wmi import WMI
from datetime import datetime
import platform
from time import sleep

class Server():
    
    def __init__(self, server_config, job_switch, data_dir_name='job_files'):
        
        # Initialize config
        self.supported_job_types = server_config['supported_job_types']
        
        # Initialize Firebase
        initialize_app(credentials.Certificate(server_config['firebase_key']), {
            'storageBucket': server_config['bucket_link']
        })
        self.bucket = storage.bucket()
        self.db = firestore.client()
        
        # Initialize tmp directory
        self.data_dir = getcwd() + '/' + data_dir_name
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        
        # Initialize server
        self.job_switch = job_switch
        self._create_server_doc()
        self._create_job_staging_listener()
        self._create_job_queue_listener()
        
        while True:
            pass
        
        
    def _fetch_file(self, job_id, cloud_name):
        print('fetching file')
        Path(self.data_dir + '/' + job_id).mkdir(parents=True, exist_ok=True)
        cloud_path = job_id + '/' + cloud_name
        blob = self.bucket.blob(cloud_path)
        print('downloading' + cloud_path)
        blob.download_to_filename(self.data_dir + '/' + cloud_path)
        print('done')
        
        
    def _create_job_queue_listener(self):
        # Create a callback on_snapshot function to capture changes
        def on_snapshot(col_snapshot, changes, read_time):
            print(f'Found [{len(changes)}] jobs in queue', flush=True)
            for change in changes:
                if change.type.name == 'ADDED':
                    job = change.document.to_dict()
                    result = self.job_switch(job['job_type'], job['args'])
                    result_doc_ref = self.db.document(f'results/{change.document.id}')
                    result_doc_ref.set({'result': result})
                    
        self.db.collection(f'servers/{self.device_name}/job_queue').on_snapshot(on_snapshot)
        
        
    def _create_job_staging_listener(self):
        # Create a callback on_snapshot function to capture changes
        def on_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'ADDED':
                    job = change.document.to_dict()
                    if job['job_type'] in self.supported_job_types:
                        # Bid on job
                        bid_ref = self.db.document(f'job_staging/{change.document.id}/available_servers/{self.device_name}')
                        bid_ref.set({})
                        
                        sleep(5)
                        
                        bid_ref = self.db.document(f'job_staging/{change.document.id}/available_servers/{self.device_name}')
                        bid_ref.set({})

        self.db.collection('job_staging').on_snapshot(on_snapshot)
        
        
    def _create_server_doc(self):
        self.device_name = platform.node()
        doc_ref = self.db.collection('servers').document(self.device_name)
        doc_ref.set({
            'supported_job_types': self.supported_job_types,
            'os': operating_system,
            'last_alive': datetime.now(),
        })
        