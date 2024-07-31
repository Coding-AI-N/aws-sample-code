import boto3
import logging
from datetime import datetime
import json

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Log the entire event object to understand its structure
        logger.info(f"Received event: {json.dumps(event)}")
        
        rds_client = boto3.client('rds')
        
        # Extract the SNS message
        sns_message = event['Records'][0]['Sns']['Message']
        logger.info(f"SNS Message: {sns_message}")
        
        # Parse the SNS message JSON
        message_details = json.loads(sns_message)
        instance_id = message_details['Source ID']
        
        # Describe the new DB instance to get its cluster identifier
        instance_info = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
        db_instance = instance_info['DBInstances'][0]
        
        if 'DBClusterIdentifier' not in db_instance:
            logger.error(f"Instance {instance_id} is not part of a cluster.")
            return
        
        cluster_id = db_instance['DBClusterIdentifier']
        
        # Describe the cluster to get its tags
        cluster_info = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
        cluster_arn = cluster_info['DBClusters'][0]['DBClusterArn']
        
        cluster_tags = rds_client.list_tags_for_resource(ResourceName=cluster_arn)['TagList']
        
        instance_arn = db_instance['DBInstanceArn']
        
        # Remove all existing tags from the instance
        instance_tags = rds_client.list_tags_for_resource(ResourceName=instance_arn)['TagList']
        if instance_tags:
            tag_keys = [tag['Key'] for tag in instance_tags]
            rds_client.remove_tags_from_resource(
                ResourceName=instance_arn,
                TagKeys=tag_keys
            )
        
        # Add tags from the cluster to the new instance
        if cluster_tags:
            rds_client.add_tags_to_resource(
                ResourceName=instance_arn,
                Tags=cluster_tags
            )
            logger.info(f"Added tags from cluster {cluster_id} to instance {instance_id} at {datetime.utcnow()}")
        else:
            logger.warning(f"No tags found for cluster {cluster_id} to add to instance {instance_id}")

        return {
            'statusCode': 200,
            'body': f'Tags added to instance {instance_id} successfully.'
        }
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        raise
