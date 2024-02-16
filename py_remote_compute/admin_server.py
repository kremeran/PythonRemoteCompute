from py_remote_compute.database.base_db import Database
from time import sleep
from threading import Event, Thread

class AdminServer:
    
    def __init__(self, database:Database, admin_config:dict):
        self.db = database
        self.admin_config = admin_config
        self.db.set_document('admin/config', admin_config)
        
    def _dispatch_job(self, doc_path:str, doc_id:str, event:Event):
        servers = self.db.get_collection('servers')
        job_obj = self.db.get_document(doc_path)
        best_server_id = None
        best_server_queue_len = None
        for server_id in servers:
            server_obj = servers[server_id]
            if job_obj['func_name'] in server_obj['available_functions'] and server_obj['alive']:
                job_queue_len = len(self.db.get_collection(f'servers/{server_id}/job_queue'))
                if best_server_queue_len is None or job_queue_len < best_server_queue_len:
                    best_server_id = server_id
                    best_server_queue_len = job_queue_len
                    
        if best_server_id is None:
            error_string = f'ADMIN: no valid compute servers found for job [{doc_id}]'
            job_obj['error'] = True
            job_obj['result'] = error_string
            self.db.set_document(f'results/{doc_id}', job_obj)
            self.db.delete_document(doc_path)
            print(error_string)
        else:
            self.db.move_document(doc_path, f'servers/{best_server_id}/job_queue/{doc_id}')
            print(f'ADMIN: dispatched [{doc_id}] to [{best_server_id}]')
        
    def _check_heartbeat(self):
        info_doc_obj = self.db.get_document('admin/info')
        info_doc_obj['time'] = self.db.timestamp_token()
        self.db.set_document('admin/info', info_doc_obj)
        server_time = self.db.get_document('admin/info')['time']
        servers = self.db.get_collection('servers')
        for server_id in servers:
            server_obj = servers[server_id]
            seconds_since_heartbeat = (server_time - server_obj['heartbeat']).total_seconds()
            if seconds_since_heartbeat < self.admin_config['heartbeat_time']-2:
                server_obj['alive'] = True
            else:
                # Server is dead. Redistribute or error out jobs if needed
                server_obj['alive'] = False
                jobs_in_queue = self.db.get_collection(f'servers/{server_id}/job_queue')
                for job_id in jobs_in_queue:
                    self.db.move_document(f'servers/{server_id}/job_queue/{job_id}', f'job_staging/{job_id}')
            self.db.set_document(f'servers/{server_id}', server_obj)
            
    def _start_heartbeat_loop(self, servers_ready:Event):
        self._check_heartbeat()
        servers_ready.set()
        sleep(self.admin_config['heartbeat_time']-2)
        while True:
            self._check_heartbeat()
            sleep(self.admin_config['heartbeat_time'])
                    
    def start(self, block:bool=True):
        
        # Create heartbeat listener. Wait for first check to complete
        servers_ready = Event()
        heartbeat_thread = Thread(daemon=True, target=self._start_heartbeat_loop, args=(servers_ready,))
        heartbeat_thread.start()
        servers_ready.wait()
        
        # Create job_staging_listener
        self.db.on_document_added('job_staging', self._dispatch_job)
        
        print('Starting admin server...')
        if block:
            while True:
                sleep(0.1)

    