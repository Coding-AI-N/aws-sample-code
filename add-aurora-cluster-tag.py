import boto3
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_tag_exists(resource_arn, key, value, rds_client):
    # Get the tags for the resource
    response = rds_client.list_tags_for_resource(ResourceName=resource_arn)
    tags = response.get('TagList', [])
    
    # Check if the tag exists
    for tag in tags:
        if tag['Key'] == key and tag['Value'] == value:
            return True
    return False

def lambda_handler(event, context):
    rds_client = boto3.client('rds')
    
    # Describe all Aurora DB clusters
    clusters = rds_client.describe_db_clusters()
    
    for cluster in clusters['DBClusters']:
        cluster_arn = cluster['DBClusterArn']
        cluster_name = cluster['DBClusterIdentifier']
        
        # Check if the tag already exists on the cluster
        if check_tag_exists(cluster_arn, 'aurora_cluster', cluster_name, rds_client):
            logger.info(f"Tag already exists on cluster {cluster_name} at {datetime.utcnow()}")
        else:
            # Add tag to the cluster
            rds_client.add_tags_to_resource(
                ResourceName=cluster_arn,
                Tags=[
                    {
                        'Key': 'aurora_cluster',
                        'Value': cluster_name
                    }
                ]
            )
            # Log the tagging action for the cluster
            logger.info(f"Tagged cluster {cluster_name} at {datetime.utcnow()}")
        
        # Describe all instances in the cluster
        instances = rds_client.describe_db_instances(Filters=[
            {
                'Name': 'db-cluster-id',
                'Values': [cluster_name]
            }
        ])
        
        for instance in instances['DBInstances']:
            instance_arn = instance['DBInstanceArn']
            
            # Check if the tag already exists on the instance
            if check_tag_exists(instance_arn, 'aurora_cluster', cluster_name, rds_client):
                logger.info(f"Tag already exists on instance {instance['DBInstanceIdentifier']} of cluster {cluster_name} at {datetime.utcnow()}")
            else:
                # Add tag to the instance
                rds_client.add_tags_to_resource(
                    ResourceName=instance_arn,
                    Tags=[
                        {
                            'Key': 'aurora_cluster',
                            'Value': cluster_name
                        }
                    ]
                )
                # Log the tagging action for the instance
                logger.info(f"Tagged instance {instance['DBInstanceIdentifier']} of cluster {cluster_name} at {datetime.utcnow()}")

    return {
        'statusCode': 200,
        'body': 'Tags added to all Aurora clusters and instances successfully.'
    }
