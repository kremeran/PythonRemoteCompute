from firebase_admin import firestore, credentials, initialize_app, storage
from os import getcwd
from pathlib import Path
from threading import Event

class Server():
    
    def __init__(self, key_path, bucket_link, data_dir_name='job_files'):
        self.data_dir = getcwd() + '/' + data_dir_name
        initialize_app(credentials.Certificate(key_path), {
            'storageBucket': bucket_link
        })
        self.bucket = storage.bucket()
        self.db = firestore.client()
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        
    def _fetch_file(self, job_id, cloud_name):
        print('fetching file')
        Path(self.data_dir + '/' + job_id).mkdir(parents=True, exist_ok=True)
        cloud_path = job_id + '/' + cloud_name
        blob = self.bucket.blob(cloud_path)
        print('downloading' + cloud_path)
        blob.download_to_filename(self.data_dir + '/' + cloud_path)
        print('done')
        
    def _run_job(self, job, job_id):
        print('Running job with id: ' + job_id)
        for file in job['job_files']:
            self._fetch_file(job_id, file['cloud_name'])
        print(job)
        
    def listen_for_jobs(self):
        print('Listening for jobs...')
        
        # Create an Event for notifying main thread.
        e = Event()

        # Create a callback on_snapshot function to capture changes
        def on_snapshot(col_snapshot, changes, read_time):
            print(f"Found [{len(changes)}] jobs in queue", flush=True)
            for change in changes:
                if change.type.name == "ADDED":
                    job = self.db.collection("compute_job_queue").document(change.document.id).get().to_dict()
                    self._run_job(job, change.document.id)

        col_query = self.db.collection("compute_job_queue")

        # Watch the collection query
        query_watch = col_query.on_snapshot(on_snapshot)
        
        while True:
            pass