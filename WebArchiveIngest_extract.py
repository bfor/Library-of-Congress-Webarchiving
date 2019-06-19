#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
from Classes_extract import *

confirm = ['yes', 'Yes', 'YES', 'y', 'Y']
deny = ['no', 'No', 'NO', 'n', 'N']
yesno = confirm + deny
DEBUG = False
extension = False
bnum = False
crawl = Crawl()
today = str(datetime.date.today())

# Pre-requisites:
# bagit.txt template file available at /sh_webcap/tags/bagit.txt (or as configured in conf.ini)
# bag-info.txt template files available in /sh_webcap/tags/[crawl-type]-bag-info.txt (or as configured)
# Network access to http://loc.archive.org/collections/

# Assumption:
# This script is invoked from /sh_webcap/[crawl-type] subdirectory.

# Check that we are logged in as the service user configured in conf.ini
if not TRANSFER:
    print "\n\nThis program was designed to be run by %s user, but you are logged in as user %s\n" % (CTSConfig['service_user'], USER)

    proceed = raw_input("Would you like to proceed?  ")
    if proceed in deny:
        print "\nOkay, exiting now\n"
        exit(0)
    elif proceed in confirm:
        out_message = "(Continuing as %s...)\n" % USER
        print out_message
        logging.debug(out_message)
    else:
        print "\nIt seems you entered an invalid option, exiting now\n"
        exit(0)


debug_check = raw_input("\n\nWould you like to enable debug mode?  ")
if debug_check in confirm:
    DEBUG = True
    logging.debug("Entering debug mode")
elif debug_check not in yesno:
    print("\nYou entered an invalid response, disabling debug mode...\n")

while not crawl.validInput:
    extension = raw_input("\nPlease enter the primary date/manifest extension: ")
    bnum = raw_input("\nPlease enter the starting bag number: ")
    if not crawl.validateInput(extension, bnum):
        print "\n\nOops, it looks like you entered a bad variable. Here's an example of valid entries:"
        print "\n	date/manifest extension:    003.12-03-2014/LOC-PRIORITY-003.manifest"
        print "	starting bag number:        100\n\n"


if CTSConfig['include_youtube_content']=='True':
    youtube_check = raw_input("\n\nYoutube content will be included. Proceed?  ")
    if youtube_check in deny:
        print "\nTo change the youtube setting, set include_youtube_content to False in conf.ini.\n\n"
        exit(0)
else:
    youtube_check = raw_input("\n\nYoutube content will NOT be included in this crawl. Proceed?  ")
    if youtube_check in deny:
        print "\nTo change the youtube setting, set include_youtube_content to True in conf.ini.\n\n"
        exit(0)

print "\n\n	Please wait, downloading manifests...\n\n"

crawl.downloadFiles()

if CTSConfig['include_youtube_content']=='True':
    logging.debug("Calling crawl.separateYouTube")
    crawl.separateYouTube()
crawl.setManifestFileData()
crawl.splitManifests()
crawl.createHoleyBags()
crawl.cleanup()

if DEBUG:
    crawl.printDebugReport()

crawl.printDetails()


initiateWorkflows = raw_input("Would you like to initiate the CTS workflows? ")
if initiateWorkflows in confirm:
    crawl.generateRequests()
    crawl.initiateCTSWorkflows()
    if crawl.error:
        print "\n\n" + crawl.error + "\n\n"
    elif crawl.msg:
        print "\n\n" + crawl.msg + "\n\n"
    else:
        print "\n\nSomething went wrong.\n\n"

    logWorkflow = "SUCCESSFULLY INITIATED %s WORKFLOWS FOR BAGS BELOW (%s):" % (len(crawl.bags), today)
    logging.info(logWorkflow)
else:
    print "\n\nSkipping workflow initiating, exiting now...\n\n"


for bag in sorted(crawl.bags):
    logData = "%s, %s files, %s" % (bag, crawl.bags[bag]['count'], today)
    logging.info(logData)
