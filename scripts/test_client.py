import configparser
from remote_compute.Client import Client

fileName = "data/Fentanyl_Addict_InterviewJake/audio.wav"
config = configparser.ConfigParser()
config.read('module_config.ini')
c = Client('firebase-key.json', 'clipai-e1d66.appspot.com')

job = {
    'job': 'transcribe',
    'args': {
        'wav_path': 'audio.wav' 
    },
    'job_files': [
        {
            'local_path': 'gettysburg10.wav',
            'cloud_name': 'audio.wav',
        }
    ],
    'config': dict(config['TRANSCRIPTION'])
}

print(c.create_job(job))