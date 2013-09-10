#!/usr/bin/python

import sys, os, apachelog, bandwidth, MySQLdb

p = apachelog.parser(r"%v:%p %h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"")
bwsum = bandwidth.Bandwidth()

for file in sys.argv[1:]:
    print "Processing %s file" % file
    for line in open(file):
        try:
            data = p.parse(line.strip())

            if (data['%O'] == '-'):
                continue

            hostname = data['%v:%p'][0:data['%v:%p'].index(':')]
            bwsum.add(hostname, data['%O'], data['%t'])
        except:
            sys.stderr.write("Unable to parse %s" % line)


connection = MySQLdb.connect(host='localhost', user='root', passwd='', db='store')
bwsum.persist(connection)