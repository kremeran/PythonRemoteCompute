from remote_compute.utils import init_firebase
import threading
from time import sleep

class Admin():
    
    def __init__(self, admin_config, firebase_key_path):
        self.admin_config = admin_config
        self.fb = init_firebase(self.admin_config, 'admin', firebase_key_path)
        self.fb.db.document('admin/config').set(self.admin_config)
        
    def clean_job_staging(self):
        job_staging = self.fb.db.collection('job_staging').get()
        if len(job_staging) > 0:
            print(f'ADMIN: Clearing {len(job_staging)} jobs from staging:')
            for job in job_staging:
                self.fb.db.document(f'job_staging/{job.id}').delete()
                print(f'--- deleted [{job.id}]')
                
    def clean_results(self):
        results = self.fb.db.collection('results').get()
        if len(results) > 0:
            print(f'ADMIN: Clearing {len(results)} results:')
            for res in results:
                self.fb.db.document(f'results/{res.id}').delete()
                print(f'--- deleted [{res.id}]')
            
    def clean_server_queue(self, server_id):
        job_queue = self.fb.db.collection(f'servers/{server_id}/job_queue').get()
        if len(job_queue) > 0:
            print(f'ADMIN: Clearing server [{server_id}] job queue:')
            for job in job_queue:
                self.fb.db.document(f'servers/{server_id}/job_queue/{job.id}').delete()
                print(f'--- deleted [{job.id}]')
            
    def clean_servers(self):
        servers = self.fb.db.collection('servers').get()
        if len(servers) > 0:
            print(f'ADMIN: Clearing {len(servers)} servers:')
            for s in servers:
                self.clean_server_queue(s.id)
                self.fb.db.document(f'servers/{s.id}').delete()
                print(f'--- deleted [{s.id}]')
                
    def clean_firebase(self):
        self.clean_job_staging()
        self.clean_servers()
        
    def _dispatch_job(self, job_id):
        servers = self.fb.db.collection('servers').get()
        job_ref = self.fb.db.document(f'job_staging/{job_id}')
        job_obj = job_ref.get().to_dict()
        best_server_id = None
        best_server_queue_len = None
        for s in servers:
            server_obj = self.fb.db.document(f'servers/{s.id}').get().to_dict()
            if job_obj['func_name'] in server_obj['available_functions']:
                job_queue_ref = self.fb.db.collection(f'servers/{s.id}/job_queue')
                queue_len = len(job_queue_ref.get())
                if best_server_queue_len is None or queue_len < best_server_queue_len:
                    best_server_id = s.id
                    best_server_queue_len = queue_len
                    
        if best_server_id is None:
            error_string = f'ADMIN: no valid compute servers found for job [{job_id}]'
            job_obj['error'] = True
            job_obj['result'] = error_string
            result_ref = self.fb.db.document(f'results/{job_id}')
            result_ref.set(job_obj)
            job_ref.delete()
            print(error_string)
        else:
            assigned_job_ref = self.fb.db.document(f'servers/{best_server_id}/job_queue/{job_id}')
            assigned_job_ref.set(job_obj)
            job_ref.delete()
            print(f'ADMIN: dispatched [{job_id}] to [{best_server_id}]')
        
    def _job_staging_listener(self):
        def on_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'ADDED':
                    self._dispatch_job(change.document.id)
        return on_snapshot
        
    def _check_heartbeat(self):
        self.fb.db.document('admin/info').set({'time': self.fb.fs.SERVER_TIMESTAMP})
        server_time = self.fb.db.document('admin/info').get().to_dict()['time']
        servers = self.fb.db.collection('servers').get()
        for s in servers:
            serv_ref = self.fb.db.document(f'servers/{s.id}')
            serv_obj = serv_ref.get().to_dict()
            seconds_since_heartbeat = (server_time - serv_obj['heartbeat']).total_seconds()
            if seconds_since_heartbeat < self.admin_config['heartbeat_time']-1:
                serv_obj['alive'] = True
            else:
                # Server is dead. TODO: Redistribute or error out jobs if needed
                serv_obj['alive'] = False
            serv_ref.set(serv_obj)
            
    def _start_heartbeat_loop(self, servers_ready):
        self._check_heartbeat()
        servers_ready.set()
        sleep(self.admin_config['heartbeat_time'])
        while True:
            self._check_heartbeat()
            sleep(self.admin_config['heartbeat_time'])
                    
    def start(self, block=True):
        
        # Create heartbeat listener. Wait for first check to complete
        servers_ready = threading.Event()
        heartbeat_thread = threading.Thread(daemon=True, target=self._start_heartbeat_loop, args=(servers_ready,))
        heartbeat_thread.start()
        servers_ready.wait()
        
        # Create job_staging_listener
        col_query = self.fb.db.collection("job_staging")
        col_query.on_snapshot(self._job_staging_listener())
        
        print('Starting admin server...')
        if block:
            while True:
                sleep(0.1)

    