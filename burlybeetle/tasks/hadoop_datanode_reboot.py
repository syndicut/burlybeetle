#!/usr/bin/env python

import time
import json

from fabric.api import env
from fabric.api import task
from fabric.api import settings

from fabric import colors
from fabric import utils
from fabric.contrib import console

from burlybeetle.http import curl_and_json


DEFAULT_INTERVAL = 5  # seconds
DEFAULT_TRIES = 360  # multiply by above is 1800: 30 minutes

NN_URL = 'http://{0}:50070'

# To be populated later
env.hosts = []
env.namenodes = []

env.stop_call = 'shutdown -r now'
env.start_call = 'true'

def get_metric(url, metric):
    return parse_metric(read_metrics(url), metric)

def parse_metric(json_content, metric_name):

    if not json_content:
        return None

    metric_name = metric_name.lower()

    for metric in json_content['beans']:
        metric_lower = metric['name'].strip().lower()
        if metric_lower == metric_name:
            return metric
    return None

def read_metrics(url):
    try:
        data = curl_and_json([url], '/jmx')
    except Exception as e:
        utils.error('Could not get data', exception=e)
    if data is None:
        raise ValueError('Could not fetch the data from provided endpoint(s)')

    return data

def active_namenode(namenodes):
    for namenode in namenodes:
        fs = get_metric(NN_URL.format(namenode),
                        'Hadoop:service=NameNode,name=FSNamesystem'
                        )
        if fs['tag.HAState'] == 'active':
            return namenode
    raise ValueError('Cannot find active namenode')

def pre_stop_callback(node):
    """Check for missing blocks"""
    if env.commit:
        wait_for_health()
    else:
        utils.puts('wait for health (noop)')

def post_stop_callback(node):
    """
    Wait until the node is back
    """
    if env.commit:
        wait_for_node(node, True)
        wait_for_node(node)
        time.sleep(10)
    else:
        utils.puts('wait for node {} (noop): to leave'.format(node))
        utils.puts('wait for node {} (noop): to come back'.format(node))


def pre_start_callback(node):
    pass


def post_start_callback(node):
    pass


# Override callbacks
env.pre_stop_callback = pre_stop_callback
env.post_stop_callback = post_stop_callback
env.pre_start_callback = pre_start_callback
env.post_start_callback = post_start_callback


@task(default=True)
def do(namenodes=['m01.delos.hdp.yandex.net', 'm02.delos.hdp.yandex.net']):
    """
    Populates the node list based on elasticsearch API information

    This will connect to a given API endpoint and possibly fill out three
    role definitions:

        * ``clients``: All elasticsearch nodes where ``master`` is false
          and ``data`` is false
        * ``data``: All elasticsearch nodes where ``data`` is true
        * ``masters``: all elasticsearch nodes where ``master`` is true
          and ``data`` is false

    Arguments:

        * ``namenodes``: Namenode FQDNs
    """
    data = None
    env.namenodes = namenodes
    active_nn = active_namenode(namenodes)
    data = get_metric(NN_URL.format(active_nn), 
            'Hadoop:service=NameNode,name=NameNodeInfo'
        )
    data_nodes = json.loads(data['LiveNodes']).keys()
    data_nodes.sort()
   

    env.hosts = data_nodes


def wait_for_node(node, leave=False):
    """
    Waits for a node to leave or join the cluster

    Continually poll namenode live nodes for the node
    """

    tries = DEFAULT_TRIES
    while tries > 0:
        utils.puts(
            'Waiting for node {} to {}'.format(
                node, 'leave' if leave else 'come back',
            )
        )
        data = get_metric(NN_URL.format(active_namenode(env.namenodes)), 
            'Hadoop:service=NameNode,name=NameNodeInfo'
        )
        live_nodes = json.loads(data['LiveNodes'])
        if node in live_nodes.keys():
            if leave:
                if live_nodes[node]['lastContact'] > 3:
                    return
            else:
                if live_nodes[node]['lastContact'] < 3:
                    return
        else:
            if leave:
                return
        tries -= 1
        time.sleep(DEFAULT_INTERVAL)
    console.confirm(
        'Node {} never {}! Press Enter to continue, '
        'CTRL+C to abort (check output of {}/dfshealth.html#tab-datanode)'.
        format(
            node,
            'left' if leave else 'came back',
            NN_URL.format(env.namenodes[0]),
        )
    )


def get_cluster_health():
    """
    Returns the current cluster health

    In elasticsearch a cluster can be either green, yellow, or red
    """

    data = get_metric(NN_URL.format(active_namenode(env.namenodes)), 
            'Hadoop:service=NameNode,name=NameNodeInfo'
        )
    return data['NumberOfMissingBlocks'] == 0


def wait_for_health():
    """
    Waits for the cluster's health to match what we want

    Continually poll the elasticsearch cluster health API for health
    to match what we want
    """

    # wait (limit * sleep) seconds
    tries = DEFAULT_TRIES
    while tries > 0:
        st = get_cluster_health()
        utils.puts(
            'Waiting for cluster health'
        )
        if st:
            return
        else:
            tries -= 1
            time.sleep(DEFAULT_INTERVAL)
    console.confirm(
        'Cluster status never got healthy! Press Enter to continue, '
        'CTRL+C to abort (check output of {}/dfshealth.html#tab-overview)'.
        format(NN_URL.format(env.namenodes[0]))
    )
