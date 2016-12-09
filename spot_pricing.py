import boto3
from datetime import datetime
import sqlite3

# Work with a sample response for development
GET_DATA = True
DB_FILE = 'cache/sample.db'

# TODO: multiple specified instance types (and their tables)
instance_type = 't1.micro'

# Store in sqlite db
#   Ignore Product, everything will be Linux/UNIX
#   Table for Instance type
#   Cols for AvailabilityZone, Timestamp, SpotPrice
#   AvailabilityZone+Timestamp primary key - identifies the entry
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

if GET_DATA:
    # Note: table names can't be parameterized
    c.execute('CREATE TABLE IF NOT EXISTS "%s" (timestamp date, availabilityzone text, spotprice real, PRIMARY KEY (timestamp, availabilityzone))' % instance_type)

    client = boto3.client('ec2')

    # Get for all Availability Zones
    # Do this in a loop to keep fetching more using NextToken
    # TODO: error handling
    next_token = ''
    while True:
        response = client.describe_spot_price_history(
            DryRun=False,
            StartTime=datetime(2016, 12, 1),
            EndTime=datetime(2016, 12, 8),
            InstanceTypes=[instance_type],
            ProductDescriptions=['Linux/UNIX'],
            NextToken=next_token
        )

        print('Fetched {} spot price entries'.format(len(response['SpotPriceHistory'])))

        # Turn response into something that's easily inserted into DB - list of tuples
        to_add = []
        for row in response['SpotPriceHistory']:
            to_add.append((row['Timestamp'], row['AvailabilityZone'], row['SpotPrice']))

        c.executemany('INSERT OR IGNORE INTO "%s" VALUES (?,?,?)' % instance_type, to_add)

        next_token = response['NextToken']
        if next_token == '':
            break

conn.commit()
conn.close()
