# ####################################################
# Python Fabric script for:
#   1. Automated Rolling Restarts of Elasticsearch
#   2. Automated Rolling Reboots of Elasticsearch nodes
# Refer https://www.elastic.co/guide/en/elasticsearch/reference/5.5/rolling-upgrades.html
# The steps in above link have been automated in this Fabric Script.
# Blame sandeepkanabar@gmail.com for any bugs
# ####################################################

################ Pre-requisites Begin ##############
## For Python2:
#pip install -v fabric==1.14.0
#pip install fabtools
 
## For Python3:
#pip install fabric3
#pip install fabtools3
 
## For python3, use http://www.pythonconverter.com/ to convert code to Python3 
## For Both Python2 & Python3:
#pip install requests

# Set the nodes_list variable to the list of VMs on which the service is supposed to run

# E.g.
# my_master_nodes = [
# 'mymasterprod1.foo.com',
# 'mymasterprod2.foo.com',
# 'mymasterprod3.foo.com',
# ]

# nodes_list = my_master_nodes
# env.user   = "sandeep_kanabar" ## replace with your username
################ Pre-requisites End ##############

################ Function Purpose Begins ##################
# test_service(service_name): 
#   Checks whether the service is up and running. service_name to be passed as argument

# check_es_connection(username, password, https, verify_hostname):
#   Checks if ES process is running on the node. 

# rolling_reboot(service_name):
#   Does rolling REBOOT of VMs on which the service passed as service_name is supposed to be running. This function should be invoked for ALL services EXCEPT elasticsearch

# rolling_reboot_es(username, password, https, verify_hostname)
#   Does rolling REBOOT of VMs on which elasticsearch service is running. To be used ONLY for Elasticsearch

# rolling_restart_es(username, password, https, verify_hostname):
# Does rolling RESTART of Elasticsearch Service on the VM it is running on. To be used ONLY for Elasticsearch
################ Function Purpose Ends ##################

################ Usage Begins ##################
# fab -f rolling_restart_es.py test_service:elasticsearch

# fab -f rolling_restart_es.py test_service:kibana

# fab -f rolling_restart_es.py rolling_reboot_es:username,password,https,verify_hostname
# fab -f rolling_restart_es.py rolling_reboot_es:elastic,changeme,False,True

# fab -f rolling_restart_es.py rolling_restart_es:username,password,https,verify_hostname
# fab -f rolling_restart_es.py rolling_restart_es:elastic,changeme,False,True

# fab -f rolling_restart_es.py check_es_connection:elastic,changeme,True,True
################ Usage Ends ####################

from fabric.api import *
import fabtools
import requests
import subprocess
from datetime import datetime
from time import sleep

class FabricException(Exception):
    pass

env.abort_exception = FabricException
USERNAME="elastic"
PASSWORD="changeme"
HEADERS = {'Content-Type': 'application/json'}
URL_PREFIX = 'http://'
URL_HTTPS_PREFIX = 'https://'
URL_SUFFIX = ':9200/'
HTTPS=False
VERIFY_HOSTNAME=True

########## NDD Non-PROD Nodes ############
es_nodes = [
    'mydatanode1.foo.com',
    'mydatanode2.foo.com',
    'mydatanode3.foo.com',
    'mymasternode1.foo.com',
]

# Set the nodes_list to be used. The one declared last will be used.
nodes_list = es_nodes
env.hosts = nodes_list
env.user   = "sandeep_kanabar"

def get_es_status():
    '''
    Gets ES cluster details viz Cluster Name, Version of ES and Cluster UUID.
    '''
    print env.host,": in get_es_status"
    try:
        response = requests.get(URL, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
        print env.host,": get_es_status returned resp code: ", response.status_code
        if response.status_code == 200:
            data = response.json()
            cluster_name = data['cluster_name']
            version = data['version']['number']
            cluster_uuid = data['cluster_uuid']
            print env.host,": cluster_name: ", cluster_name
            print env.host,": version: ", version
            print env.host,": cluster_uuid: ", cluster_uuid
    except requests.exceptions.RequestException as e:
        print env.host," get_es_status: ", e

def get_cluster_settings():
    '''
    Gets ES cluster settings. 
    The most important one for rolling restarts is the transient.cluster.routing.allocation.enable. 
    If the value is "all", it's enabled. If "none", it's disabled.
    "all" means movement of shards across nodes is allowed.
    "none" means movement of shards across nodes is disabled.
    Before doing rolling restart of a node, we set this value to "none" i.e disable movement of shards
    Once node is up, we enable it back.
    '''
    print env.host,": in get_cluster_settings"    
    url=URL+"_cluster/settings?pretty"
    print env.host,": url is: ", url
    try:
        response = requests.get(url, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
        print env.host,": get_cluster_settings returned resp code: ", response.status_code
        if response.status_code == 200:
            data = response.json()
            print "data: ", data
            if keys_exists(data,"transient","cluster","routing","allocation","enable"):
                transient_settings = data['transient']['cluster']['routing']['allocation']['enable']
                print "transient_settings: ", transient_settings

            if keys_exists(data,"transient","cluster","routing","allocation","disable"):
                disable_settings = data['transient']['cluster']['routing']['allocation']['disable']
                print "disable_settings: ", disable_settings
    except requests.exceptions.RequestException as e:
        print env.host," get_cluster_settings: ", e

def change_cluster_settings(alloction_enable):
    '''
    Change ES cluster settings. 
    Changes the value of transient.cluster.routing.allocation.enable to "none" or "all" as passed in argument.
    '''
    #user_input = prompt("change_cluster_settings - Enter Y/N to proceed")
    #if user_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
    url=URL+"_cluster/settings?pretty"
    print env.host,": url is: ", url
    post_body = '{\"transient\": { \"cluster.routing.allocation.enable\": \"%s\" }}'
    post_body = post_body%alloction_enable
    while True:
        try:
            response = requests.put(url, headers=HEADERS, data=post_body, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
            print env.host,": change_cluster_settings returned resp code: ", response.status_code
            if response.status_code == 200:
                print response.text
                data = response.json()
                status = data['acknowledged']
                print env.host,": status: ", status
                break
        except requests.exceptions.RequestException as e:
            print env.host," change_cluster_settings: ", e
        sleep(10)

def put_flush_syncd():
    '''
    Issue a flush synced request so that buffers are flushed. 
    '''
    #user_input = prompt("put_flush_syncd - Enter Y/N to proceed")
    #if user_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
    url=URL+"_flush/synced?pretty"
    print env.host,": url is: ", url
    try:
        response = requests.post(url, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
        print env.host,": put_flush_syncd returned resp code: ", response.status_code
        ##print response.text ###- this prints too much data. Hence commenting
        if response.status_code == 200:
            data = response.json()
            status = data['_shards']['failed']
            print env.host,": status: ", status
        ## Issue a flush sync'd second time. It's harmless
        response = requests.post(url, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
        print env.host,": put_flush_syncd returned resp code: ", response.status_code
        if response.status_code == 200:
            data = response.json()
            status = data['_shards']['failed']
            print env.host,": status:", status
    except requests.exceptions.RequestException as e:
        print env.host," change_cluster_settings: ", e

def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
    #if type(element) is not dict:
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

def check_user_input():
    user_input = prompt("Do you want to proceed: ")
    if user_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
        return "yes"
    
def check_node_is_up(node_name):
    '''
    Checks if ES service in node is running or not after restart of node. Keeps checking until ES service is up
    '''
    url=URL+"_cat/nodes?h=name&pretty"
    print env.host,": url is: ", url
    response=''
    while True:
        try:
            response = requests.get(url, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
        except requests.exceptions.RequestException as e:
            print env.host," Error: ", e
            sleep(10)
            continue
        break
    print env.host,":", response.text
    return node_name in response.text

def check_cluster_status():
    '''
    Checks if Cluster status is Green. Keeps checking until it is green upon which it returns "green"
    While checking cluster status, it also displays the count of Unassigned Shards.
    '''
    #url=URL+"_cat/health?h=status&pretty"
    url=URL+"_cluster/health?pretty"
    print env.host,": url is: ", url
    response=''
    #post_body = '{\"transient\": { \"cluster.routing.allocation.enable\": \"all\" }}'
    while True:    
        try:
            response = requests.get(url, auth=(USERNAME,PASSWORD), verify=VERIFY_HOSTNAME)
            if response.status_code == 200:
                data = response.json()
                status = data['status']
                unassigned_shards = data['unassigned_shards']
                #print env.host,": status: ", status, " : Unassigned Shards: ", unassigned_shards
                msg = ":: Status:  " + status + "   :: Unassigned Shards:   " + str(unassigned_shards)
                print_log(msg)
        except requests.exceptions.RequestException as e:
            print "Error: ", e
            sleep(10)
            continue
        break
    #print env.host,":", response.text
    return "green" in status

def test_service_up(service_name):
    print "check if process", service_name, "is running"
    print fabtools.service.is_running(service_name)
    ##sudo("systemctl status " + service_name)

def test_connection():
    try:
        run('ls')
        return True
    except Exception:
        return False

def rolling_reboot_process(service_name):
    '''
    This function does rolling Reboot of VMs and checks if service is up
    To be used for rolling reboot of Logstash, Kibana and any service EXCEPT Elasticsaearch
    '''
    
    print "rr_reboot_process for",service_name
    print "Processing host:", env.host

    if fabtools.service.is_running(service_name):
        print "Process", service_name, "is running so will stop it"
        status = sudo("systemctl stop " + service_name)
        print "succ: ", status.succeeded
        print "fail: ", status.failed            
        
    sleep(10)
        
    if not fabtools.service.is_running(service_name): 
        print "Process", service_name, "is NOT running so will go and reboot"
        try:
            sudo("reboot")
        except FabricException:
            pass

    sleep(30)
    
    print "Testing if system is up after reboot"
    while not test_connection():
        sleep(10)
    
    print "ls code succeeded. System is up. Just for safety, sleeping a bit more"
    sleep(30)
    
    print env.host,": Checking if", service_name, "is up"    
    while not fabtools.service.is_running(service_name): 
        sleep (10)
    print env.host,":", service_name, "is up and running" 

def rolling_reboot_es_process():
    '''
    This function does rolling Reboot of VMs and not only checks if the ES service is up but also checks that ES cluster is in GREEN state before proceeding to reboot next VM. 
    To be used ONLY for rolling reboot of Elasticsearch Service
    '''
    #pdb.set_trace()
    print env.host,": rolling_reboot_es_process"    

    print env.host,"check if iptables is running. If not, exit"    
    if not fabtools.service.is_running("iptables"): 
        exit()
        
    get_es_status()
    get_cluster_settings()
    if "data" in env.host:
        change_cluster_settings("none")
        get_cluster_settings()
        put_flush_syncd()

    print env.host,": stopping ES service" 
    status = sudo("systemctl stop elasticsearch-es-01")
    print env.host,": ES stop succ: ", status.succeeded
    print env.host,": ES stop fail: ", status.failed

    print env.host,": rebooting System" 
    try:
        sudo("reboot")
    except FabricException:
        pass

    sleep(30)
    
    print env.host,": Testing if system is up after reboot"
    while not test_connection():
        sleep(10)
    
    print env.host,": ls code succeeded. System is up. Just for safety, sleeping a bit more"
    sleep(30)

    ## sleep until the ES service in node is up
    while not check_node_is_up(env.host.split('.')[0]):
        sleep(5)
    
    if "data" in env.host:
        change_cluster_settings("all")
    
    ## sleep until cluster is in green state
    while not check_cluster_status():
        sleep(10)

def rolling_restart_es_process():
    '''
    This function does rolling restart of ES Cluster. 
    It not only checks if the ES service is up but also checks that ES cluster is in GREEN state before proceeding to restart Elasticsearch service on next VM. 
    To be used ONLY for rolling restart of Elasticsearch Service
    '''
    
    #pdb.set_trace()
    print env.host,": rolling_restart_es_process"        
    
    get_es_status()
    get_cluster_settings()
    if "data" in env.host: 
        print env.host,": This is Data Node. Hence disabling shard allocation before restart."
        change_cluster_settings("none")
        get_cluster_settings()
        put_flush_syncd()

    print env.host,": restaring ES service"    
    status = sudo("systemctl restart elasticsearch-es-01")
    print env.host,": ES stop succ: ", status.succeeded
    print env.host,": ES stop fail: ", status.failed
    
    while not check_node_is_up(env.host.split('.')[0]):
        sleep(5)
        
    if "data" in env.host:        
        print env.host,": This is Data Node. Hence enabling back shard allocation after restart."
        change_cluster_settings("all")

    while not check_cluster_status():
        sleep(10)

def rolling_reboot(service_name):
    rolling_reboot_process(service_name)

def test_service(service_name):
    test_service_up(service_name)

def rolling_reboot_es(username, password, https, verify_hostname):
    global USERNAME,PASSWORD,URL,HTTPS,VERIFY_HOSTNAME
    USERNAME=username
    PASSWORD=password

    HTTPS = True if https == "True" else False

    VERIFY_HOSTNAME=True if verify_hostname == "True" else False
    
    prefix = URL_HTTPS_PREFIX if HTTPS else URL_PREFIX
    URL=prefix+env.hosts[0]+URL_SUFFIX

    print "rr_reboot_es::USERNAME: ", USERNAME
    print "rr_reboot_es::PASSWORD: ", PASSWORD
    print "rr_reboot_es::URL: ",URL
    print "rr_reboot_es::HTTPS: ",HTTPS
    print "rr_reboot_es::VERIFY_HOSTNAME:",VERIFY_HOSTNAME

    rolling_reboot_es_process()

def rolling_restart_es(username, password, https, verify_hostname):
    global USERNAME,PASSWORD,URL,HTTPS,VERIFY_HOSTNAME
    USERNAME=username
    PASSWORD=password
    
    HTTPS = True if https == "True" else False

    VERIFY_HOSTNAME=True if verify_hostname == "True" else False
    
    prefix = URL_HTTPS_PREFIX if HTTPS else URL_PREFIX
    URL=prefix+env.hosts[0]+URL_SUFFIX

    print "rr_restart_es::USERNAME: ", USERNAME
    print "rr_restart_es::PASSWORD: ", PASSWORD
    print "rr_restart_es::URL: ",URL
    print "rr_restart_es::HTTPS: ",HTTPS
    print "rr_restart_es::VERIFY_HOSTNAME:",VERIFY_HOSTNAME

    rolling_restart_es_process()

def check_process_running(process_name):
    print "check if process", process_name, "is running"
    check_process_is_running(process_name)

def check_es_connection(username, password, https, verify_hostname):
    '''
    Checks if ES process is running on the node. 
    This can be used to check if ES process is up and running on the node(s)
    '''
    global USERNAME,PASSWORD,URL,HTTPS,VERIFY_HOSTNAME
    USERNAME=username
    PASSWORD=password

    HTTPS = True if https == "True" else False

    VERIFY_HOSTNAME=True if verify_hostname == "True" else False

    prefix = URL_HTTPS_PREFIX if HTTPS else URL_PREFIX
    URL=prefix+env.host+URL_SUFFIX

    print "====================================================================="
    print "check_es_connection:: ", URL
        
    get_es_status()

def print_log(msg):
    '''
    Prints the msg passed as argument along with timestamp and hostname
    '''
    print datetime.now().strftime("%Y-%m-%d %H:%M:%S"), env.host, msg
