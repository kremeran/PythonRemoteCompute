import configparser
from context import Client
from datetime import datetime

fileName = "data/Fentanyl_Addict_InterviewJake/audio.wav"
config = configparser.ConfigParser()
config.read('module_config.ini')
c = Client('firebase-key.json', 'clipai-e1d66.appspot.com')

job = {
    'job_type': 'reverse_string',
    'args': [
        'Hello World!',
    ],
    'creation_time': datetime.now()
    # 'job_files': [
    #     {
    #         'local_path': 'gettysburg10.wav',
    #         'cloud_name': 'audio.wav',
    #     }
    # ],
}

print(c.create_job(job))