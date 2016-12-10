# Usage: specify params (time interval, instance types, avail zones)
# Does: Looks up from cache 1st; fetches results that aren't in the cache
#   Check if instance type is present
#   Gets present time range: have latest early time thru earliest late time; assumes present time ranges are contiguous
import boto3
from datetime import datetime, timedelta
import sqlite3
import numpy as np
import matplotlib.pyplot as plt

# Work with a sample response for development
DB_FILE = 'cache/sample.db'

TIME_STR = '%Y-%m-%d %H:%M:%S'


def fetch_spot_history(conn, instance_type, start_time, end_time):
    # https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.Client.describe_spot_price_history
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-spot-price-history.html
    # Get for all Availability Zones
    # Do this in a loop to keep fetching more using NextToken
    # TODO: error handling
    client = boto3.client('ec2')
    next_token = ''
    while True:
        response = client.describe_spot_price_history(
            DryRun=False,
            StartTime=start_time,
            EndTime=end_time,
            InstanceTypes=[instance_type],
            ProductDescriptions=['Linux/UNIX'],
            NextToken=next_token
        )

        print('Fetched {} spot price entries'.format(len(response['SpotPriceHistory'])))

        # Turn response into something that's easily inserted into DB - list of tuples
        #   By default, returned datetime has timezone info. It's UTF from the AWS API.
        #    Strip it for compatibility with sqlite
        to_add = []
        for row in response['SpotPriceHistory']:
            to_add.append((row['Timestamp'].replace(tzinfo=None), row['AvailabilityZone'], row['SpotPrice']))

        with conn:
            conn.executemany('INSERT OR IGNORE INTO "{table}" VALUES (?,?,?)'.format(table=instance_type), to_add)

        next_token = response['NextToken']
        if next_token == '':
            break


def get_avail_zones(conn, instance_type):
    c = conn.cursor()
    zones = []
    c.execute('SELECT DISTINCT availabilityzone FROM "{table}"'.format(table=instance_type))
    for row in c:
        zones.append(row[0])
    return zones


def update_spot_history(instance_type, start_time, end_time):
    #
    # Store in sqlite db
    #   Ignore Product, everything will be Linux/UNIX
    #   Table for Instance type
    #   Cols for AvailabilityZone, Timestamp, SpotPrice
    #   AvailabilityZone+Timestamp primary key - identifies the entry
    # Fetches all avail zones together, which may cause extra fetches when avail zones times don't perfectly overlap
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    # Note: table names can't be parameterized
    with conn:
        conn.execute('CREATE TABLE IF NOT EXISTS "{table}" (timestamp TIMESTAMP , availabilityzone TEXT, spotprice REAL, PRIMARY KEY (timestamp, availabilityzone))'.format(table=instance_type))

    # Get availability zones present
    zones = get_avail_zones(conn, instance_type)

    if zones:  # empty?
        # Get time range already present in the database
        first_times = []
        last_times = []
        c = conn.cursor()
        for zone in zones:
            c.execute('SELECT MIN(timestamp) FROM "{table}" WHERE availabilityzone=?'.format(table=instance_type), (zone,))
            first_times.append(datetime.strptime(c.fetchone()[0], TIME_STR))
            c.execute('SELECT MAX(timestamp) FROM "{table}" WHERE availabilityzone=?'.format(table=instance_type), (zone,))
            last_times.append(datetime.strptime(c.fetchone()[0], TIME_STR))

        first_time = max(first_times)
        last_time = min(last_times)

        # Fetch earlier windows if needed
        if start_time < first_time:
            fetch_spot_history(conn, instance_type, start_time, first_time)

        # Fetch later windows if needed
        if last_time < end_time:
            fetch_spot_history(conn, instance_type, last_time, end_time)

    conn.close()


if __name__ == '__main__':
    GET_DATA = False  # DEBUG: whether to collect/update data at all

    # Get the current date/time and 1 week ago
    # Turn into UTC
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=60)

    # TODO: multiple specified instance types (and their tables)
    instance_type = 't1.micro'

    if GET_DATA:
        update_spot_history(instance_type, start_time, end_time)

    # Analyze data
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    # Make plots of spot price over time for each avail zone
    # This is probably inefficient...
    zones = get_avail_zones(conn, instance_type)

    # Exclude us-east-1e zone. It looks like a test zone that has too-high price
    try:
        zones.remove('us-east-1e')
    except ValueError:
        pass

    times = []
    prices = []
    c = conn.cursor()
    for zone in zones:
        c.execute('SELECT timestamp, spotprice FROM "{table}" WHERE availabilityzone=? ORDER BY timestamp'.format(table=instance_type), (zone,))
        times_i = []
        prices_i = []
        for row in c:
            times_i.append(row[0])
            prices_i.append(row[1])

        times.append(times_i)
        prices.append(prices_i)

    plt.figure()
    for i, zone in enumerate(zones):
        plt.plot(times[i], prices[i])
    plt.legend(zones)
    plt.show()
