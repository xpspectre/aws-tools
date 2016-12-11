# Usage: specify params (time interval, instance types, avail zones)
# Does: Looks up from cache 1st; fetches results that aren't in the cache
#   Check if instance type is present
#   Gets present time range: have latest early time thru earliest late time; assumes present time ranges are contiguous
import os
import boto3
from datetime import datetime, timedelta
import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Work with a sample response for development
CACHE_DIR = 'cache'
TIME_STR = '%Y-%m-%d %H:%M:%S'


def fetch_spot_history(conn, instance_type, availability_zone, start_time, end_time):
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
            AvailabilityZone=availability_zone,
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
            conn.executemany('INSERT OR IGNORE INTO history VALUES (?,?,?)', to_add)

        next_token = response['NextToken']
        if next_token == '':
            break


def get_db_avail_zones(conn, instance_type):
    c = conn.cursor()
    zones = []
    c.execute('SELECT DISTINCT availabilityzone FROM history')
    for row in c:
        zones.append(row[0])
    return zones


def update_spot_history(instance_type, start_time, end_time):
    # Update cached spot price history
    # Store in sqlite db, 1 db for each instance type
    #   Ignore Product, everything will be Linux/UNIX
    #   Single table called 'history'
    #   Cols for AvailabilityZone, Timestamp, SpotPrice
    #   AvailabilityZone+Timestamp primary key - identifies the entry
    # Fetches all avail zones together, which may cause extra fetches when avail zones times don't perfectly overlap
    db_file = os.path.join(CACHE_DIR, '{}.db'.format(instance_type))
    conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    # Note: table names can't be parametrized
    with conn:
        conn.execute('CREATE TABLE IF NOT EXISTS history (timestamp TIMESTAMP , availabilityzone TEXT, spotprice REAL, PRIMARY KEY (timestamp, availabilityzone))')

    # Get availability zones remotely
    client = boto3.client('ec2')
    response = client.describe_availability_zones()
    zones = []
    for row in response['AvailabilityZones']:
        zones.append(row['ZoneName'])

    # Get availability zones in cache
    c = conn.cursor()
    cached_zones = get_db_avail_zones(conn, instance_type)

    for zone in zones:
        if zone in cached_zones:
            c.execute('SELECT MIN(timestamp) FROM history WHERE availabilityzone=?', (zone,))
            first_time = datetime.strptime(c.fetchone()[0], TIME_STR)
            c.execute('SELECT MAX(timestamp) FROM history WHERE availabilityzone=?', (zone,))
            last_time = datetime.strptime(c.fetchone()[0], TIME_STR)

            # Fetch earlier windows if needed
            if start_time < first_time:
                fetch_spot_history(conn, instance_type, zone, start_time, first_time)

            # Fetch later windows if needed
            if last_time < end_time:
                fetch_spot_history(conn, instance_type, zone, last_time, end_time)
        else:
            fetch_spot_history(conn, instance_type, zone, start_time, end_time)

    conn.close()


def get_spot_history(instance_type, start_time, end_time, exclude_zones=[]):
    # Get spot price history from cache. Returns times and prices as numpy datatypes.
    db_file = os.path.join(CACHE_DIR, '{}.db'.format(instance_type))
    conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    times = []
    prices = []
    c = conn.cursor()
    zones = get_db_avail_zones(conn, instance_type)

    for exclude_zone in exclude_zones:
        try:
            zones.remove(exclude_zone)
        except ValueError:
            pass

    for zone in zones:
        c.execute('SELECT timestamp, spotprice FROM history WHERE availabilityzone=? AND timestamp BETWEEN datetime(?) AND datetime(?) ORDER BY timestamp', (zone, start_time, end_time))
        times_i = []
        prices_i = []
        for row in c:
            times_i.append(row[0])
            prices_i.append(row[1])

        times.append(np.array(times_i, dtype='datetime64[s]'))  # units of seconds
        prices.append(np.array(prices_i))
    return times, prices, zones


if __name__ == '__main__':
    GET_DATA = True  # DEBUG: whether to collect/update data at all

    # Get the current date/time and 1 week ago
    # Turn into UTC
    # end_time = datetime.utcnow()
    end_time = datetime(2016, 12, 10)
    start_time = end_time - timedelta(days=7)

    instance_type = 't1.micro'

    if GET_DATA:
        update_spot_history(instance_type, start_time, end_time)

    # Analyze data
    # Make plots of spot price over time for each avail zone
    # This is probably inefficient...
    times, prices, zones = get_spot_history(instance_type, start_time, end_time, exclude_zones=['us-east-1e'])

    plt.figure()
    for i, zone in enumerate(zones):
        plt.plot(times[i], prices[i])
    plt.legend(zones)
    plt.title(instance_type)
    plt.show()
