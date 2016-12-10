import csv
import math

INSTANCE_TABLE = 'aws_instance_types.csv'


def load_instance_types():
    # Load instance properties list
    #   There doesn't seem to be an automatic way of doing this, and not all intances are available in all regions
    #   But can get spot market instance availability
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
            instances.append({'name': name, 'cpu': cpu, 'ecu': ecu, 'mem': mem, 'disk': disk})
    return instances


def get_valid_instance_types(resource_req):
    #   Get list of instances and the number of units each instance type can give you
    #   Doesn't explicitly pay attention to inefficient packing - this will get sorted out depending on price/unit
    instances = load_instance_types()
    instance_units = []
    for type in instances:
        ncpu = math.floor(type['cpu'] / resource_req['cpu'])  # how many units can pack into CPUs
        nmem = math.floor(type['mem'] / resource_req['mem'])  # how many units can pack into memory
        n = min(ncpu, nmem)
        if n >= 1:
            name = type['name']
            instance_units.append({'name': name, 'nunits': n})
    return instance_units

if __name__ == '__main__':
    # Request resources
    resource_req = {'cpu': 8, 'mem': 16.0}  # constitutes a single "unit" of request

    instance_units = get_valid_instance_types(resource_req)
    for instance_unit in instance_units:
        print(instance_unit)
