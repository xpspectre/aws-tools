import csv
import math

# Load instance properties list
#   There doesn't seem to be an automatic way of doing this, and not all intances are available in all regions
#   But can get spot market instance availability

INSTANCE_TABLE = 'aws_instance_types.csv'

instances = []
with open(INSTANCE_TABLE, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader, None)  # skip the headers
    next(csvreader, None)
    for row in csvreader:
        name = row[0]
        cpu = int(row[1])
        try:
            ecu = float(row[2])
        except ValueError:
            ecu = float('nan')
        mem = float(row[3])
        disk = row[4]  # arbitrary string for EBS, multiple SSDs, etc
        instances.append([name, cpu, ecu, mem, disk])

print(instances)

# Request resources
#   Get list of instances and the number of units each instance type can give you
#   Doesn't explicitly pay attention to inefficient packing - this will get sorted out depending on price/unit
resource_req = {'cpu': 1, 'mem': 2.0}  # constitutes a single "unit" of request

instance_units = []
for type in instances:
    ncpu = math.floor(type[1]/resource_req['cpu'])  # how many units can pack into CPUs
    nmem = math.floor(type[3]/resource_req['mem'])  # how many units can pack into memory
    n = min(ncpu, nmem)
    if n >= 1:
        name = type[0]
        instance_units.append([name, n])

print(instance_units)

