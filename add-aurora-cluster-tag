import boto3
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    rds_client = boto3.client('rds')
    
    # Describe all Aurora DB clusters
    clusters = rds_client.describe_db_clusters()
    
    for cluster in clusters['DBClusters']:
        cluster_arn = cluster['DBClusterArn']
        cluster_name = cluster['DBClusterIdentifier']
        
        # Add tag to the cluster
        rds_client.add_tags_to_resource(
            ResourceName=cluster_arn,
            Tags=[
                {
                    'Key': 'cluster',
                    'Value': cluster_name
                }
            ]
        )
        # Log the tagging action for the cluster
        logger.info(f"Tagged cluster {cluster_name} at {datetime.utcnow()}")

        # Describe all instances in the cluster
        instances = rds_client.describe_db_instances()
        
        for instance in instances['DBInstances']:
            # Ensure instance is part of the current cluster
            if 'DBClusterIdentifier' in instance and instance['DBClusterIdentifier'] == cluster_name:
                instance_arn = instance['DBInstanceArn']
                
                # Add tag to the instance
                rds_client.add_tags_to_resource(
                    ResourceName=instance_arn,
                    Tags=[
                        {
                            'Key': 'cluster',
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
