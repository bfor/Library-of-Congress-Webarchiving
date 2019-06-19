#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import math
import glob
import shutil
import datetime
from textwrap import dedent
from Conf import *

class Crawl():
    def __init__(self):
	self.validInput = False

    #commented out in WebArchiveIngest for fedgovextract bag creation
    def validateInput(self, extension=None, bnum=None):
	if not extension or not bnum:
	    return False

	# Bag number is an integer that helps create a unique, sequential bag ID in CTS.
	# The user of the script will look up in a spreadsheet or in CTS to determine which
	# bag number to use here.
	# This script will generate as many bags as required to keep the bag size under 1TB
	# and download the entire crawl.
	try:
	    int(bnum)
	except:
	    self.badBnum = True
	    logging.error("Unable to parse bag number %s" % bnum)
	    return False


	# Example of valid extension input: 003.12-03-2014/LOC-PRIORITY-003.manifest
	pathSplit = extension.split('/')
	if len(pathSplit) == 2:
	    numDateSplit = pathSplit[0].split('.')
	    if len(numDateSplit) == 2:
		# crawlNum is the first part of the extension input: a three-digit number
		# that identifies the crawl within its collection on the IA site
		crawlNum = numDateSplit[0]
		if len(crawlNum) == 3:
		    try:
			int(crawlNum)
		    except:
			self.badExtension = True
			logging.error("Unable to parse crawl number %s" % crawlNum)
			return False

		    date = numDateSplit[1]
		    dateSplit = date.split('-')
		    if len(dateSplit) == 3:
			manPathSplit = pathSplit[1].split('.')
			if len(manPathSplit) == 2:
			    if manPathSplit[1] == 'manifest':
				self.validInput = True
				self.setVariables(extension, bnum)
				return True

	logging.error("Got bad extension input: %s" % extension)
	self.badExtension = True
	return False

    def setVariables(self, extension, bnum):
	    # Example of extension input: 003.12-03-2014/LOC-PRIORITY-003.manifest
	    self.extension = extension
	    self.bnum = int(bnum)

	    
	    # USFEDGOV-EXTRACT transfer input: USFEDGOV-EXTRACT_manifest.txt
	    
	    
	    
	    
	    
	    
	    
	    # The first part of the extension, e.g. 003.12-03-2014
	    self.crawlnumdate = extension.split("/")[0]
	    # e.g. 2014
	    self.yyyy = self.crawlnumdate.split("-")[2]
	    self.yy = self.yyyy[-2:]
	    self.mm = self.crawlnumdate.split("-")[0].split(".")[1]
	    self.dd = self.crawlnumdate.split("-")[1]

	    # The three-digit crawl number (as a string)
	    self.crawlnum = self.crawlnumdate.split('.')[0]

	    
	    # The manifest file piece of the extension, e.g. LOC-PRIORITY-003.manifest
	    self.manifest = extension.split("/")[1]

	    
	    # The CDX version of user-input extension, e.g. 003.12-03-2014/LOC-PRIORITY-003-CDX.manifest
	    self.manifestCDX = '%s/%s' % (self.crawlnumdate,
					  self.manifest.replace('.manifest', '-CDX.manifest'))

	    # e.g. LOC-PRIORITY-003.manifest.tmp
	    self.manifestT = self.manifest + ".tmp"

	    # e.g. LOC-PRIORITY-003
	    self.crawlID = self.manifest.split(".")[0]

	    # e.g. 003.12-03-2014/crawl-report.txt
	    self.report = self.crawlnumdate + "/crawl-report.txt"

	    # Collection /crawl type, e.g. priority, weekly, monthly
	    self.type = self.manifest.split("-")[1].lower()

	    # These Youtube-Related members are set if the include_youtube_content option is True
	    self.vidBags = 0
	    self.vidSplitLen = 0
	    self.vidSize = 0
	    self.manifestYTFile = False

	    self.requestsGenerated = False
	    self.error = False
	    self.msg = False
	    self.bags = {}

	    if 'election' in self.type:
		election_year = self.type.replace('election', '')
		el_yy = election_year[-2:]
		self.type = 'el' + el_yy
		self.baseURL = CTSConfig['archive_url'] + 'e' + el_yy + '/stats/'
	    else:
                # 'oy3_' only for old crawls 2019-06-19; remove once they are transferred
		self.baseURL = CTSConfig['archive_url'] + 'oy3_' + self.type + '/stats/'

	    if 'monthly' == self.type:
		self.date =  self.yyyy + self.mm
	    else:
		self.date = self.yyyy + self.mm + self.dd


    def downloadFiles(self):
	
	
	self.manifestTempFile = IAReport(
	    self.manifestT,
	    self.baseURL + self.extension
	)

	self.crawlreportFile = IAReport(
	    'crawlreport.txt',
	    self.baseURL + self.report
	)

	self.manifestCDXFile = IAReport(
	    'manifest-cdx.txt',
	    self.baseURL + self.manifestCDX
	)

	if self.manifestTempFile.download()==False:
	    print self.manifestTempFile.error
	    exit(0)
	    
	if self.crawlreportFile.download()==False:
            print self.crawlreportFile.error
	    exit(0)

	if self.manifestCDXFile.download()==False:
	    print self.manifestCDXFile.error
	    exit(0)

	self.size = self.crawlreportFile.size
	self.byte = self.crawlreportFile.byte

    def separateYouTube(self):
	### YOUTUBE-RELATED ###

	# The youtube version of the user-input extension, e.g. 003.12-03-2014/LOC-PRIORITY-003-YOUTUBE.manifest
	extensionV = self.extension.replace('.manifest', '-YOUTUBE.manifest')

	# e.g. LOC-PRIORITY-003-YOUTUBE.manifest
	manifestV = extensionV.split("/")[1]

	self.manifestYTFile = self.manifestTempFile.separateYouTube(self.manifest, manifestV)

    def setManifestFileData(self):
	if self.manifestYTFile:
	    self.vidSize = ((float(self.manifestYTFile.lineCount)/self.manifestTempFile.lineCount) * self.size)
	    self.vidBags = self.divBags(self.vidSize)
	    self.vidSplitLen = (self.manifestYTFile.lineCount/self.vidBags + 2)
	    self.manifestFile = IAReport(self.manifest)
	    self.manifestFile.getLineCount()
	else:
	    self.manifestFile = self.manifestTempFile

	self.mainManLines = self.manifestFile.lineCount
	self.mainSize = ((float(self.mainManLines)/self.manifestTempFile.lineCount) * self.size)
	self.mainBags = self.divBags(self.mainSize)
	self.mainSplitLen = (self.mainManLines/self.mainBags + 2)

    def divBags(self, size):
	if self.byte == 'GB':
	    size = (size/1024)
	conv = (size/.95)
	return int(math.ceil(conv))

    def splitManifests(self):
	if self.manifestYTFile:
	    self.manifestYTFile.splitManifest(self.vidSplitLen)

	self.manifestFile.splitManifest(self.mainSplitLen)

    def createHoleyBags(self, path=None):
	if path:
	    self.path = path
	else:
	    self.path = CTSConfig['stagingfilepath']

	self.allmans = glob.glob(self.path + '/manifest*')
	tag = CTSConfig['tags_folder'] + self.type + '-bag-info.txt'
	bagit = CTSConfig['bagit_txt_file']
	n = 0

	for man in self.allmans:
	    n += 1

	    if 'manifest-cdx.txt' in man:
		bag = 'waindex_%s_%s' % (self.type, self.crawlnum)
	    else:
		if 'el' in self.type:
		    bag = self.type + '_' + str(self.bnum) + '_' + self.date
		else:
		    bag = self.type + str(self.bnum) + '_' + self.date

		if 'YT' in man:
		    bag += '_yt'

		self.bnum += 1

	    baginfo = '/'.join([self.path, bag, 'bag-info.txt'])
	    bagIt = '/'.join([self.path, bag, 'bagit.txt'])

	    try:
		os.makedirs(bag)
		os.makedirs(bag + '/' + 'data')
	    except:
		print "\n\nERROR: It appears that bag %s  S for this crawl/bag number combination has already been processed\n\n" % bag
		self.cleanup()
		exit(0)
	    count = 0

	    fetch = '/'.join([self.path, bag, 'fetch.txt'])
	    md5 = '/'.join([self.path, bag, 'manifest-md5.txt'])

	    with open(fetch, 'a') as fetchFile:
		with open(md5, 'a') as md5File:
		    with open(man, 'r') as manFile:
			for line in manFile.readlines():
			    count += 1
			    field = line.split()
			    fetchFile.write(field[2] + ' -  data/' + field[0] + '\n')
			    md5File.write(field[1] + ' data/' + field[0] + '\n')

	    self.bags[bag] = {'count': count}
	    bagIt = '/'.join([self.path, bag, 'bagit.txt'])
	    shutil.copy(bagit, bagIt)

	    # Make sure tags exists
	    if not os.path.isfile(tag):
		print "\n\nERROR: a tag file for this crawl type does not yet exist.  As transfer user, please create %s\n\n" % tag
		self.cleanup()
		exit(0)

	    with open(baginfo, 'w') as output:
		with open(tag, 'r') as input:
		    for line in input:
			line = line.replace('Crawl-ID:', 'Crawl-ID: ' + self.crawlID)
			line = line.replace('Internal-Sender-Identifier:', 'Internal-Sender-Identifier: ' + bag)
			output.write(line)

    def generateRequests(self):
	for bag in self.bags:
	    if 'waindex_' in bag:
		typeBag = 'waindexes/%s' % bag
	    else:
		typeBag = '%s/%s' % (self.type, bag)

	    bagWorkflowData = [
		'variable.bagId=' + bag,
		'variable.stagingFilepath=' + CTSConfig['stagingfilepath'] + bag,
		'variable.longTermStorageFilepath=' + CTSConfig['longtermstoragefilepath'] + typeBag,
		'variable.accessFilepath=' + CTSConfig['accessfilepath'] + typeBag
	    ]

	    self.bags[bag]['request'] = '&'.join(CTSConfig['data'] + bagWorkflowData)

	self.requestsGenerated = True

    def initiateCTSWorkflows(self):
	if not self.requestsGenerated:
	    self.error = "ERROR: You must use the Crawl.generateRequests() function before initiating requests..."
	    return False

	if not CTSConfig['workflowurl']:
	    self.error = "No API url detected"
	    return False

	else:
	    self.msg = ''
	    url = CTSConfig['workflowurl']
	    for bag in sorted(self.bags):
		request = self.bags[bag]['request']
		try:
		    urllib2.urlopen(url, request)
		except:
		    self.error = "Failed workflow initiation on bag %s with [%s]" % (bag, request)
		    return False

		self.msg += "	%s\n" % bag

	self.msg = "Successfully initiated workflows for the following bags:\n\n" + self.msg
	return True

    def printDebugReport(self):
	if self.manifestYTFile:
	    self.youtubeLineCount = self.manifestYTFile.lineCount
	else:
	    self.youtubeLineCount = 0

	report = """

		 ------------
		 DEBUG INFO:
		 ------------

		 Date Manifest Extension = %s
		 Manifest URL = %s
		 Report URL = %s
		 Manifest File Name = %s
		 Manifest Line Count = %s
		 YouTube Content Split = %s

		 Crawl Type = %s
		 Crawl ID = %s
		 Crawl Size = %s %s
		  +Main Content Size = %s %s
		  +YouTube Content Size = %s %s

		 Total Number of Processed Bags = %s

		 """ % (
		     self.extension,
		     self.baseURL + self.extension,
		     self.baseURL + self.report,
		     self.manifest,
		     self.manifestFile.lineCount,
		     self.youtubeLineCount,
		     self.type,
		     self.crawlID,
		     self.size, self.byte,
		     self.mainSize, self.byte,
		     self.vidSize, self.byte,
		     len(self.bags)
		 )

	print dedent(report)

    def printDetails(self):
	bags = ''
	buffer = ' ' * 19
	n = 0
	for bag in sorted(self.bags):
	    n += 1
	    bags += "\n%s%s --- %s  (%s files)" % (
		buffer,
		str(n).ljust(2),
		bag.ljust(30),
		self.bags[bag]['count']
	    )

	report = """

		 -------------------------
		 -=:PROCESSING COMPLETE:=-
		 -------------------------

		 CRAWL NUMBER: %s

		 PATHS:
		   +Staging Directory:		%s		(%s)
		   +Destination Directory:	%s		(%s)
		   +Access copy to:		%s	(%s)

		 BAGS: %s

		 """ % (
		     self.crawlnum,
		     CTSConfig['stagingfilepath'], CTSConfig['staging'],
		     CTSConfig['longtermstoragefilepath'], CTSConfig['longterm'],
		     CTSConfig['accessfilepath'], CTSConfig['access'],
		     bags
		 )

	print dedent(report)

    def cleanup(self):
	locs = glob.glob(self.path + '/LOC-*')
	crawlreport = self.path + '/crawlreport.txt'

	os.remove(crawlreport)
	for file in self.allmans:
	    os.remove(file)
	for file in locs:
	    os.remove(file)

class IAReport():
    def __init__(self, filename=None, url=None):
	self.filename = filename
	self.url = url

    def download(self):
	if self.filename and self.url:
	    try:
	        urlData = urllib2.urlopen(self.url).read()
	    except:
		self.error = "Could not download the specified resource (%s)" % self.url
		return False

	    with open(self.filename, 'w') as localFile:
		localFile.write(urlData)

	    if 'crawlreport' in self.filename:
		self.getSize()
	    elif 'manifest' in self.filename:
		self.getLineCount()

	    return True
	else:
	    self.error = "Improper filename (%s) or url (%s) provided" % (self.filename, self.url)
	    return False

    def getSize(self):
	self.size = False
	self.byte = False
	search = open(self.filename)
	for line in search:
	    if 'Compressed WARC' in line:
		self.byte = line.replace(')', '').split()[-1]

		#if crawl-report.txt shows a sub 1GB bag, default to 1GB
		if self.byte != 'KB':
		    self.size = line.replace('(', '').split()[-2]
		    self.size = float(self.size)
		else:
		    self.byte = 'GB'
		    self.size = float(1)


    def getLineCount(self):
	with open(self.filename) as file:
	    self.lineCount = sum(1 for _ in file)
	   # for i, l in enumerate(file):
	       # pass	   
       # self.lineCount = int(i + 1)

    def separateYouTube(self, manifest, manifestV):
	self.youtube = False

	logging.debug("Youtube option is %s " % CTSConfig['include_youtube_content'])

	with open(self.filename, 'r') as input:
	    with open(manifest, 'a') as man:
		with open(manifestV, 'a') as manYT:
		    for line in input:
			if 'YOUTUBE' in line and CTSConfig['include_youtube_content']=='True':
			    self.youtube = True
			    manYT.write(line)
			elif 'YOUTUBE' not in line:
			    man.write(line)

	if self.youtube:
	    manifestYTFile = IAReport(manifestV)
	    manifestYTFile.youtube = True
	    manifestYTFile.getLineCount()
	    return manifestYTFile
	else:
	    return False

    def splitManifest(self, splitLen):
	if 'YOUTUBE' in self.filename and CTSConfig['include_youtube_content']=='True':
	    self.splitName = 'manifestYT'
	elif 'YOUTUBE' not in self.filename:
	    self.splitName = 'manifest'

	count = 1
	input = open(self.filename, 'r').read().split('\n')
	outputFile = self.splitName + str(count) + '.txt'

	for lines in range(0, len(input), splitLen):
	    outputData = input[lines:lines + splitLen]
	    outputFile = self.splitName + str(count) + '.txt'
	    output = open(outputFile, 'w')
	    output.write('\n'.join(outputData))
	    output.close()
	    count += 1
