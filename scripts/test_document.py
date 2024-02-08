from firebase_admin import firestore, credentials, initialize_app, storage

initialize_app(credentials.Certificate('firebase-key.json'), {
    'storageBucket': 'clipai-e1d66.appspot.com'
})
bucket = storage.bucket()
db = firestore.client()

doc_ref = db.collection("test").document()