# https://aws.amazon.com/sdk-for-python/
# https://github.com/boto/boto3
# https://boto3.readthedocs.io/en/latest/
from datetime import datetime, timedelta
from instance_calc import get_valid_instance_types
from spot_pricing import update_spot_history, get_spot_history
import matplotlib.pyplot as plt

if __name__ == '__main__':
    GET_DATA = False
    SHOW_PLOTS = True

    # Request resources
    resource_req = {'cpu': 8, 'mem': 16.0}  # constitutes a single "unit" of request

    instances = get_valid_instance_types(resource_req)

    end_time = datetime.utcnow()
    # end_time = datetime(2016, 12, 10)
    start_time = end_time - timedelta(days=7)

    # TODO: These can be done in parallel
    if GET_DATA:
        for instance in instances:
            instance_type = instance['name']
            update_spot_history(instance_type, start_time, end_time)

    # Calculate $/unit for each instance type, availability zone
    #   us-east-1e zone looks weird
    # TODO: Get on-demand price for comparison
    #   https://aws.amazon.com/blogs/aws/new-aws-price-list-api/
    #   https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html
    for i, instance in enumerate(instances):
        instance_type = instance['name']
        times, prices, zones = get_spot_history(instance_type, end_time - timedelta(days=5), end_time, exclude_zones=['us-east-1e'])  # Test out shorter window

        if SHOW_PLOTS:
            plt.figure(i)
            for i, zone in enumerate(zones):
                plt.plot(times[i], prices[i])
            plt.legend(zones)
            plt.title(instance_type)
            plt.draw()

    if SHOW_PLOTS:
        plt.show()
