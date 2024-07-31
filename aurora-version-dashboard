import boto3
import json
import datetime
from collections import Counter

def lambda_handler(event, context):
    rds_client = boto3.client('rds')
    cloudwatch_client = boto3.client('cloudwatch')
    
    clusters = rds_client.describe_db_clusters()['DBClusters']
    
    version_counter = Counter()
    detailed_metric_data = []
    
    for cluster in clusters:
        cluster_id = cluster['DBClusterIdentifier']
        engine_version = cluster['EngineVersion']
        
        # Count versions for summary metrics
        version_counter[engine_version] += 1
        
        # Prepare detailed metrics for each cluster
        detailed_metric_data.append({
            'MetricName': 'ClusterVersionDetail',
            'Dimensions': [
                {
                    'Name': 'ClusterId',
                    'Value': cluster_id
                },
                {
                    'Name': 'EngineVersion',
                    'Value': engine_version
                }
            ],
            'Timestamp': datetime.datetime.utcnow(),
            'Value': 1,  # Static value for counting
            'Unit': 'Count'
        })
    
    # Prepare summary metrics
    summary_metric_data = []
    for version, count in version_counter.items():
        summary_metric_data.append({
            'MetricName': 'ClusterVersionCount',
            'Dimensions': [
                {
                    'Name': 'EngineVersion',
                    'Value': version
                },
            ],
            'Timestamp': datetime.datetime.utcnow(),
            'Value': count,
            'Unit': 'Count'
        })
    
    # Push both detailed and summary metrics to CloudWatch
    cloudwatch_client.put_metric_data(
        Namespace='AuroraClusters',
        MetricData=detailed_metric_data
    )
    
    cloudwatch_client.put_metric_data(
        Namespace='AuroraClusters',
        MetricData=summary_metric_data
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully pushed cluster versions to CloudWatch')
    }
