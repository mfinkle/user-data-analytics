import csv
from datetime import datetime
import psycopg2
import sys

if len(sys.argv) != 3:
    print "Arg count: " + str(len(sys.argv) - 1)
    print "Expected <channel> <file>"
    sys.exit(1)

print "Importing: " + sys.argv[2] + " into " + sys.argv[1]
con = None

channel = sys.argv[1]
if channel != "beta" and channel != "nightly":
    print "Unknown channel. Expected 'beta' or 'nightly'"
    sys.exit(1)

databaseName = "fennec-telemetry"
if channel == "nightly":
    databaseName = "fennec-telemetry-nightly"
try:
    con = psycopg2.connect(database=databaseName, user="mfinkle")
    cur = con.cursor()

    upsertSQL = ("INSERT INTO "
                 "clients(clientid, channel, profile_created, last_submission, current_appversion, current_osversion, memory) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                 "ON CONFLICT (clientid) DO "
                 "UPDATE SET "
                 "profile_created = EXCLUDED.profile_created, "
                 "last_submission = EXCLUDED.last_submission, "
                 "current_appversion = EXCLUDED.current_appversion, "
                 "current_osversion = EXCLUDED.current_osversion, "
                 "memory = EXCLUDED.memory")

    with open(sys.argv[2], "rb") as csvfile:
        clients = csv.DictReader(csvfile)
        for row in clients:
            clientid = row["clientid"]
            channel = row["channel"]
            profileDate = row["profiledate"]
            if profileDate != "":
                profileDate = datetime.strptime(profileDate, "%Y%m%d")
            else:
                profileDate = None
            submissionDate = datetime.strptime(row["submissiondate"], "%Y%m%d")

            appVersion = int(row["version"].split(".")[0])
            osVersion = int(row["osversion"])
            memory = row["memory"]

            cur.execute(upsertSQL, (clientid, channel, profileDate, submissionDate, appVersion, osVersion, memory))

    con.commit()

except psycopg2.DatabaseError, e:
    if con:
        con.rollback()

    print "Error %s" % e
    sys.exit(1)

finally:
    if con:
        con.close()