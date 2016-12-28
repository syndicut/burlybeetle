#!/usr/bin/env python

import time
import json

from fabric.api import env
from fabric.api import task
from fabric.api import settings
from fabric.api import lcd
from fabric.api import prefix
from fabric.api import quiet
from fabric.api import local
from fabric.api import execute
from fabric.api import hide
from fabric.api import run

from fabric import colors
from fabric import utils
from fabric.contrib import console

from burlybeetle.http import curl_and_json, curl


DEFAULT_INTERVAL = 5  # seconds
DEFAULT_TRIES = 420  # multiply by above is 2100: 35 minutes

NN_URL = 'http://{0}:50070'
HM_URL = 'http://{0}:60010'
RM_ENDPOINT = ':8088/ws/v1/cluster/nodes'

NM_THRESHOLD = 60

# Paramiko slowness fix
env.disable_known_hosts = True

# To be populated later
env.hosts = []
env.namenodes = []
env.hbase_masters = []
env.resource_managers = []
env.roledefs['regionservers'] = []
env.roledefs['nodemanagers'] = []

env.stop_call = 'shutdown -r now'
env.start_call = 'true'

#env.prev_commit = env.commit

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
    data = json.loads(curl([url], '/jmx'))
    if data is None:
        raise ValueError('Could not fetch the data from provided endpoint(s)')

    return data

def active_namenode(namenodes):
    tries = DEFAULT_TRIES
    while tries > 0:
        for namenode in namenodes:
            try:
                fs = get_metric(NN_URL.format(namenode),
                            'Hadoop:service=NameNode,name=FSNamesystem'
                            )
            except Exception:
                continue
            if 'tag.HAState' in fs and fs['tag.HAState'] == 'active':
                return namenode
        utils.puts('Cannot find active namenode, retrying...')
        tries -= 1
        time.sleep(DEFAULT_INTERVAL)
    raise ValueError('Cannot find active namenode')

def active_hbase_master(masters):
    tries = DEFAULT_TRIES
    while tries > 0:
        for master in masters:
            try:
                info = get_metric(HM_URL.format(master),
                            'Hadoop:service=HBase,name=Master,sub=Server'
                            )
            except Exception:
                continue
            if 'tag.isActiveMaster' in info and \
               info['tag.isActiveMaster'] == 'true':
                return master
        utils.puts('Cannot find active hbase master, retrying...')
        tries -= 1
        time.sleep(DEFAULT_INTERVAL)
    raise ValueError('Cannot find active hbase master')

def get_master(cluster_path):
    '''
    Try to get connection string to salt master.
    Return array with connection strings or empty array
    if detection failed
    '''
    with lcd("{0}/bin".format(cluster_path)), \
         prefix('source ../conf/salt-config.sh'), \
         quiet():
        ssh_user = local('echo $SSH_USER', capture=True)
        salt_master = local('echo $SALT_MASTER', capture=True)
        if len(ssh_user) > 0:
            ret = '{0}@{1}'.format(ssh_user, salt_master)
        else:
            ret = salt_master
        if len(salt_master) > 0:
            return [ret]
        else:
            return []

def get_inventory_from_master():
    with hide('stdout'):
        res = run('reclass -o json -b /srv/salt/inventory/ -i')
    if res.succeeded:
        inventory = json.loads(res)
        return inventory

def enable_balancer():
    run('echo balance_switch true | hbase shell')

def disable_balancer():
    run('echo balance_switch false | hbase shell')

def get_inventory(cluster_path):
    res = execute(get_inventory_from_master, hosts=get_master(cluster_path))
    _, inventory = res.popitem()
    return inventory

def get_namenodes(inventory):
    return inventory['applications']['hadoop.hdfs.namenode']

def get_hbase_masters(inventory):
    if 'hadoop.hbase.master' in inventory['applications']:
        return inventory['applications']['hadoop.hbase.master']
    else:
        return []

def get_resource_managers(inventory):
    if 'hadoop.yarn.resourcemanager' in inventory['applications']:
        return inventory['applications']['hadoop.yarn.resourcemanager']
    else:
        return []

def pre_stop_callback(node):
    """Check for missing blocks"""
    if env.commit:
        wait_for_health()
        with lcd(env.cluster_path):
            local('bin/bootstrap-salt-minion.sh {0}'.format(node))
        run('apt-get install config-caching-dns')
        with settings(warn_only=True):
            local('jctl http://jmon.paysys.yandex.net:8998/api-slb add downtime -host {0} -stop +1hours'.format(node))
        if node in env.roledefs['nodemanagers']:
            run('/etc/init.d/hadoop-yarn-nodemanager stop')
        if node in env.roledefs['regionservers']:
            disable_balancer()
            run('/usr/bin/hbase org.jruby.Main /usr/lib/hbase/bin/region_mover.rb -f /root/regions unload $(hostname -f)')
    #    if node in env.namenodes or \
    #       node in env.hbase_masters or \
    #       node in env.resource_managers:
    #       utils.puts('Skipping node {0} found in masters list'.format(node))
    #       env.prev_commit = True
    #       env.commit = False
    else:
        utils.puts('wait for health (noop)')

def post_stop_callback(node):
    """
    Wait until the node is back
    """
    if env.commit:
        wait_for_node(node, True)
        time.sleep(30)
    else:
        utils.puts('wait for node {} (noop): to leave'.format(node))
        utils.puts('wait for node {} (noop): to come back'.format(node))
    #if node in env.namenodes or \
    #   node in env.hbase_masters or \
    #   node in env.resource_managers:
    #   utils.puts('Skipping node {0} found in masters list'.format(node))
    #   env.commit = env.prev_commit


def pre_start_callback(node):
    if env.commit:
        wait_for_ping(node)
        time.sleep(150)
        with settings(connection_attempts=60):
            # Workaround for buggy datanode script
            run("pkill -f org.apache.hadoop.hdfs.server.datanode.DataNode")
            run("for pkg in $(dpkg -l | awk '$3 ~ /cdh5/ {print $2}'); do init=/etc/init.d/${pkg}; if [ -f $init ]; then $init restart; fi; done")
        wait_for_node(node)
        if node in env.roledefs['regionservers']:
            region_move_cmd = '/usr/bin/hbase org.jruby.Main /usr/lib/hbase/bin/region_mover.rb -f /root/regions load $(hostname -f)'
            with settings(warn_only = True):
                res = run(region_move_cmd)
            if not res.succeeded:
                run(region_move_cmd)
            enable_balancer()
        with settings(warn_only=True):
            local('jctl http://jmon.paysys.yandex.net:8998/api-slb modify downtime -host {0} -stop +10minutes'.format(node))


def post_start_callback(node):
    pass

# Override callbacks
env.pre_stop_callback = pre_stop_callback
env.post_stop_callback = post_stop_callback
env.pre_start_callback = pre_start_callback
env.post_start_callback = post_start_callback

@task(default=True)
def do(cluster_path, skip=0):
    """
    Populates the node list based on namenode API information

    Arguments:

        * ``cluster_path``: Path to cluster repo
    """
    env.cluster_path = cluster_path
    env.inventory = get_inventory(cluster_path)
    namenodes = get_namenodes(env.inventory)
    hbase_masters = get_hbase_masters(env.inventory)
    resource_managers = get_resource_managers(env.inventory)

    utils.puts('Found namenodes: {0} for cluster path {1}'.format(namenodes, cluster_path))
    utils.puts('Found hbase masters: {0} for cluster path {1}'.format(hbase_masters, cluster_path))
    utils.puts('Found resource managers: {0} for cluster path {1}'.format(resource_managers, cluster_path))
    data = None
    env.namenodes = namenodes
    env.hbase_masters = hbase_masters
    env.resource_managers = resource_managers
    active_nn = active_namenode(namenodes)

    data = get_metric(NN_URL.format(active_nn), 
            'Hadoop:service=NameNode,name=NameNodeInfo'
        )
    data_nodes = json.loads(data['LiveNodes']).keys()
    data_nodes.sort()

    if len(hbase_masters) > 0:
        active_hm = active_hbase_master(hbase_masters)
        data = get_metric(HM_URL.format(active_hm),
                'Hadoop:service=HBase,name=Master,sub=Server'
            )
        regionserver_list = data['tag.liveRegionServers'].split(';')
        regionserver_nodes = map(lambda rs: rs.split(',')[0], regionserver_list)
        env.roledefs['regionservers'] = regionserver_nodes

    if len(resource_managers) > 0:
        rm_apis = map(lambda rm: 'http://{0}'.format(rm), resource_managers)
        data = curl_and_json(rm_apis, RM_ENDPOINT)
        nm_nodes = map(lambda node: node['nodeHostName'], data['nodes']['node'])
        env.roledefs['nodemanagers'] = nm_nodes

    env.hosts = data_nodes[int(skip):]
    with lcd(env.cluster_path):
        local('git pull --rebase')
        with lcd('bin'):
            local('git pull --rebase origin master')
        with settings(warn_only=True):
            local('git commit -m "Updated bin" bin')
        local('git push')


def get_dn_status(node):
    data = get_metric(NN_URL.format(active_namenode(env.namenodes)), 
            'Hadoop:service=NameNode,name=NameNodeInfo'
        )
    live_nodes = json.loads(data['LiveNodes'])
    if node in live_nodes.keys():
        if live_nodes[node]['lastContact'] > 3:
            utils.puts('Datanode at {0} last contact time > 3'.format(node))
            return False
        else:
            utils.puts('Datanode at {0} is alive'.format(node))
            return True
    else:
        utils.puts('Datanode at {0} is not found in live nodes list'.format(node))
        return False

def get_rs_status(node):
    if node not in env.roledefs['regionservers']:
        raise ValueError('Node {0} not found in RS list, skipping'.format(node))
    data = get_metric(HM_URL.format(active_hbase_master(env.hbase_masters)),
            'Hadoop:service=HBase,name=Master,sub=Server'
        )
    regionserver_list = data['tag.liveRegionServers'].split(';')
    regionserver_nodes = map(lambda rs: rs.split(',')[0], regionserver_list)
    if node in regionserver_nodes:
        utils.puts('Node {0} is found in live RS list'.format(node))
        return True
    else:
        utils.puts('Node {0} is not found in live RS list'.format(node))
        return False

def get_rm_status(node):
    if node not in env.roledefs['nodemanagers']:
        raise ValueError('Node {0} not found in nodemanager list, skipping'.format(node))
    rm_apis = map(lambda rm: 'http://{0}'.format(rm), env.resource_managers)
    data = curl_and_json(rm_apis, RM_ENDPOINT)
    for nodemanager in data['nodes']['node']:
        if nodemanager['nodeHostName'] == node and nodemanager['state'] == 'RUNNING':
            utils.puts('Node {0} is found in running nodemanager list'.format(node))
            if time.time() - nodemanager['lastHealthUpdate'] / 1000 < NM_THRESHOLD:
                utils.puts('Node {0} lastHealthUpdate is less then threshold {1}'.format(node, NM_THRESHOLD))
                return True
            else:
                utils.puts('Node {0} lastHealthUpdate is more then threshold {1}'.format(node, NM_THRESHOLD))
                return False
    utils.puts('Node {0} is not found in running nodemanager list'.format(node))
    return False

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
        dn_status = get_dn_status(node)
        try:
            rs_status = get_rs_status(node)
        except ValueError:
            rs_status = not leave
        try:
            rm_status = get_rm_status(node)
        except ValueError:
            rm_status = not leave
        if leave:
            if not (dn_status or rs_status or rm_status):
                return
        else:
            if dn_status and rs_status and rm_status:
                return
        tries -= 1
        time.sleep(DEFAULT_INTERVAL)
    console.confirm(
        'Node {} never {}! Press Enter to continue, '
        'CTRL+C to abort'.
        format(
            node,
            'left' if leave else 'came back',
            NN_URL.format(env.namenodes[0]),
        )
    )

def wait_for_ping(node):
    """
    Waits for a ping to node to pass
    """

    tries = DEFAULT_TRIES
    while tries > 0:
        utils.puts(
            'Waiting for ping to node {}'.format(
                node,
            )
        )
        with settings(warn_only=True):
            res = local('ping6 -c3 {}'.format(node))
        if res.succeeded:
            return
        tries -= 1
        time.sleep(DEFAULT_INTERVAL)
    console.confirm(
        'Node {} never pinged! Press Enter to continue, '
        'CTRL+C to abort'.
        format(
            node,
        )
    )

def get_cluster_health():
    """
    Returns the current cluster health
    """
    # TODO(syndicut): Check for backup master of hbase and backup namenode
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
