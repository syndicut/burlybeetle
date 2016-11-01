#!/usr/bin/env python

from fabric.api import env

# Import the top-level tasks
from tasks import *

# Import the per-module tasks as top-level tasks
from tasks import elasticsearch, hadoop_datanode_reboot

assert elasticsearch
assert hadoop_datanode_reboot

env.use_ssh_config = True
env.shell = '/bin/bash -l -o pipefail -c'
env.colorize_errors = True
env.linewise = True
