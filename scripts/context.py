import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from remote_compute.Client import Client
from remote_compute.Server import Server
from remote_compute.Admin import Admin
from remote_compute.utils import init_firebase