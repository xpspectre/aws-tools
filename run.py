# https://aws.amazon.com/sdk-for-python/
# https://github.com/boto/boto3
# https://boto3.readthedocs.io/en/latest/
from datetime import datetime, timedelta
from instance_calc import get_valid_instance_types
from spot_pricing import update_spot_history, get_spot_history
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    values = []
    for i, instance in enumerate(instances):
        instance_type = instance['name']
        nunits = instance['nunits']
        times, prices, zones = get_spot_history(instance_type, end_time - timedelta(days=5), end_time, exclude_zones=['us-east-1e'])  # Test out shorter window

        if SHOW_PLOTS and len(zones) > 0:
            plt.figure(i)
            ax = plt.gca()
            for j, zone in enumerate(zones):
                plt.plot(times[j].astype(datetime), prices[j])
            xax = ax.get_xaxis()
            xax.set_major_formatter(mdates.DateFormatter('%b %d %H:%M'))  # Still squashed together
            plt.legend(zones)
            plt.title(instance_type)
            plt.draw()

        price_per_units = []
        for j, zone in enumerate(zones):
            price_per_units.append(prices[j]/nunits)

        # Save everything and/or calculate some sort of useful stats
        values.append({'name': instance_type, 'times': times, 'price_per_unit': price_per_units, 'zones': zones})

    if SHOW_PLOTS:
        plt.show()

    1
