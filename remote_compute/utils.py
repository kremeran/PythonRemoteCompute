from firebase_admin import firestore, credentials, initialize_app, storage

def get_admin_config(server_id, firebase_key_path):
    name = server_id + '_get_admin_config'
    app = initialize_app(credentials.Certificate(firebase_key_path), name=name)
    db = firestore.client(app=app)
    admin_config = db.document('admin/config').get().to_dict()
    return admin_config

def init_firebase(admin_config, app_name, firebase_key_path):
    class FirebaseObj():
        app, bucket, db, fs = None, None, None, None
    
    fb = FirebaseObj()
    if admin_config['file_support']:
        if admin_config['folder_path']:
            fb.app = initialize_app(credentials.Certificate(firebase_key_path), {
                'storageBucket': admin_config['folder_path']
            }, name=app_name)
            fb.bucket = storage.bucket(app=fb.app)
        else:
            raise ValueError('<file_support> enabled but no <folder_path> provided')
    else:
        fb.app = initialize_app(credentials.Certificate(firebase_key_path), name=app_name)
        fb.bucket = None
    fb.db = firestore.client(app=fb.app)
    fb.fs = firestore
    
    return fb