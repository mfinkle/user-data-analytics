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

def insertBuffer(cursor, rows, sql):
    values = ",".join(["%s"] * len(rows))
    insert = sql.format(values)
    cursor.execute(insert, rows)
    #print cursor.mogrify(insert, rows)

try:
    con = psycopg2.connect(database=databaseName, user="mfinkle")
    cur = con.cursor()

    insertSQL = ("INSERT INTO "
                 "events(clientid, submission, timestamp, action, method, extras, experiments) "
                 "VALUES {0} "
                 "ON CONFLICT ON CONSTRAINT events_unique DO NOTHING")

    with open(sys.argv[2], "rb") as csvfile:
        bufferRows = []
        bufferCount = 10

        rowCount = 0
        events = csv.DictReader(csvfile)
        for row in events:
            clientid = row["clientid"]
            submissionDate = row["submissiondate"]
            timestamp = row["timestamp"]
            action = row["action"][0:30]
            method = row["method"][0:30]
            extras = row["extras"][0:128]
            experiments = row["experiments"][0:128]

            rowCount += 1

            bufferRows.append((clientid, submissionDate, timestamp, action, method, extras, experiments))
            if len(bufferRows) == bufferCount:
                insertBuffer(cur, bufferRows, insertSQL)
                bufferRows = []

                output = "Inserted rows: " + str(rowCount)
                sys.stdout.write("\r" + output)
                sys.stdout.flush()

        if len(bufferRows) > 0:
            insertBuffer(cur, bufferRows, insertSQL)
            bufferRows = []

    con.commit()

    print "-"
    print "Total inserted rows: " + str(rowCount)

except psycopg2.DatabaseError, e:
    if con:
        con.rollback()

    print "Error %s" % e
    sys.exit(1)

finally:
    if con:
        con.close()