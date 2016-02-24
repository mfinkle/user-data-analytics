import csv
from datetime import datetime
import sys, traceback

if len(sys.argv) != 2:
    print "Arg count: " + str(len(sys.argv))
    sys.exit(1)

print "Validating: " + sys.argv[1]

constraints = {}
totalLines = 0

try:
    with open(sys.argv[1], "rb") as csvfile:
        events = csv.DictReader(csvfile)
        for row in events:
            clientid = row["clientid"]
            submissionDate = row["submissiondate"]
            timestamp = row["timestamp"]
            action = row["action"][0:30]
            method = row["method"][0:30]
            extras = row["extras"][0:128]

            key = clientid + timestamp + action + method
            if not key in constraints:
              constraints[key] = 0
            constraints[key] += 1
            totalLines += 1

    print "Total count: " + str(totalLines)
    print "Unique count: " + str(len(constraints))
    #for key, value in constraints:
    #    print key + " : " + value

except:
    e = sys.exc_info()[0]
    print "Error %s" % e
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
