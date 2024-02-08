from context import Server

def reverse_string(input_string):
    print('conferting')
    return input_string[::-1]

def job_switch(job_type, args):
    match job_type:
        case 'reverse_string':
            result = reverse_string(args[0])
            return result
        case _:
            # Move to result queue with error string as result
            print('Invalid job type')

server_config = {
    'firebase_key': 'firebase-key.json',
    'bucket_link': 'clipai-e1d66.appspot.com',
    'supported_job_types': ['reverse_string']
}

Server(server_config, job_switch)
