# https://aws.amazon.com/sdk-for-python/
# https://github.com/boto/boto3
# https://boto3.readthedocs.io/en/latest/
from datetime import datetime, timedelta
from instance_calc import get_valid_instance_types
from spot_pricing import update_spot_history

if __name__ == '__main__':
    GET_DATA = False

    # Request resources
    resource_req = {'cpu': 8, 'mem': 16.0}  # constitutes a single "unit" of request

    instance_units = get_valid_instance_types(resource_req)

    # end_time = datetime.utcnow()
    end_time = datetime(2016, 12, 10)
    start_time = end_time - timedelta(days=7)

    # TODO: These can be done in parallel
    if GET_DATA:
        for instance_unit in instance_units:
            instance_type = instance_unit['name']
            update_spot_history(instance_type, start_time, end_time)

    # Calculate $/unit for each instance type, availability zone

