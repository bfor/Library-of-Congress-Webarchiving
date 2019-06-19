#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import urllib
import urllib2
import getpass
import ConfigParser
import logging

global CTSConfig, USER, TRANSFER
USER = getpass.getuser()
TRANSFER = False

#Register CTS Configurations
Config = ConfigParser.ConfigParser()
Config.read('/home/transfer/webarchive_staging_workflow/conf.ini')
CTSConfig = dict(Config.items('CTS-WebArchive-Workflow'))
CTSConfig['data'] = [
    'processDefinitionId=webarchive1',
    'variable.isTest=' + CTSConfig['test'],
    'variable.projectId=webcap',
    'variable.requiredDiskSpaceInTB=1',
    'variable.stagingStorageSystemId=' + CTSConfig['staging'],
    'variable.longTermStorageSystemId=' + CTSConfig['longterm'],
    'variable.performCopyToAccess=true',
    'variable.accessStorageSystemId=' + CTSConfig['access'],
    ]
CTSConfig['stagingfilepath'] = os.getcwd() + '/'
CTSConfig['requests'] = []

if USER == CTSConfig['service_user']:
    TRANSFER = True


# Register CTS API request handler
passwordManager = urllib2.HTTPPasswordMgrWithDefaultRealm()
passwordManager.add_password(
    None,
    CTSConfig['workflowurl'],
    CTSConfig['user'],
    CTSConfig['password']
    )
handler = urllib2.HTTPBasicAuthHandler(passwordManager)
opener = urllib2.build_opener(handler)
opener.addheaders = [('Accept', 'application/json')]
urllib2.install_opener(opener)

# Configure logging
logging.basicConfig(
    format='%(message)s',
    filename=CTSConfig['logfile'],
    level=logging.DEBUG
)
