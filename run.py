# https://aws.amazon.com/sdk-for-python/
# https://github.com/boto/boto3
# https://boto3.readthedocs.io/en/latest/
import boto3

ec2 = boto3.resource('ec2')

# Get instance statuses
# for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
#     print(status)

# Get running instances
instances = ec2.instances.filter(
    Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
for instance in instances:
    print(instance.id, instance.instance_type)