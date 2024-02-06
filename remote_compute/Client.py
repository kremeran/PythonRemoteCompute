from firebase_admin import firestore, credentials, initialize_app, storage

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

    def create_job(self, job):
        doc_ref = self.db.collection("compute_job_queue").document()
        job_id = doc_ref.id
        for file in job['job_files']:
            cloud_name = file['cloud_name']
            self._store_file(file['local_path'], f'{job_id}/{cloud_name}')
        doc_ref.set(job)
        return job_id
