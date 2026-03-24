"""
WSGI entry point for PythonAnywhere deployment.
Account: zziai40
Path: /home/zziai40/ProjectPSLCricket
"""

import sys
import os

project_home = '/home/zziai40/ProjectPSLCricket'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

os.chdir(project_home)

from app import app as application
