from firebase_admin import firestore, credentials, initialize_app, storage
from time import sleep
import threading

class Client():
    
    def __init__(self, key_path, bucket_link):
        initialize_app(credentials.Certificate(key_path), {
            'storageBucket': bucket_link
        })
        self.bucket = storage.bucket()
        self.db = firestore.client()

    def _store_file(self, local_path, cloud_path):
        blob = self.bucket.blob(cloud_path)
        blob.upload_from_filename(local_path)
        
    def _wait_for_response(self, job_id):
        print('waiting for response...')
        response_done = threading.Event()

        # Create a callback on_snapshot function to capture changes
        def on_snapshot(doc_snapshot, changes, read_time):
            for change in changes:
                if change.type.name == 'ADDED':
                    print(f"Received document snapshot: {change.document.id}")
                    print(change.document.to_dict())
                    response_done.set()

        doc_ref = self.db.collection("results").document(job_id)

        # Watch the document
        doc_watch = doc_ref.on_snapshot(on_snapshot)
        response_done.wait()

    def create_job(self, job):
        doc_ref = self.db.collection("job_staging").document()
        job_id = doc_ref.id
        if 'job_files' in job:
            for file in job['job_files']:
                cloud_name = file['cloud_name']
                self._store_file(file['local_path'], f'{job_id}/{cloud_name}')
        doc_ref.set(job)
        
        ### SERVER BIDS FOR JOB ###
        sleep(3)
        
        available_servers = self.db.collection(f'job_staging/{job_id}/available_servers').get()
        for server_doc in available_servers:
            print(f'Running on server [{server_doc.id}]')
            job_queue_ref = self.db.document(f'servers/{server_doc.id}/job_queue/{job_id}')
            job_queue_ref.set(job)
            
            # Remove job from staging
            doc_ref.delete()
            break
        
        self._wait_for_response(job_id)
        
        return job_id
