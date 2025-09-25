import boto3
from PIL import Image
import io
s3 = boto3.client('s3')
sns = boto3.client('sns')
INPUT_BUCKET = 'data-input-2026'
OUTPUT_BUCKET = 'data-output-2025'
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:147205849078:DataProcessingNotifications'
def lambda_handler(event, context):
bucket = event['Records'][0]['s3']['bucket']['name']
key = event['Records'][0]['s3']['object']['key']
if bucket != INPUT_BUCKET:
print(f"Ignored file from {bucket}")
return
response = s3.get_object(Bucket=bucket, Key=key)
img_data = response['Body'].read()
image = Image.open(io.BytesIO(img_data))
# Resize to 200x200
image = image.resize((200, 200))
out_buffer = io.BytesIO()
fmt = image.format if image.format else "PNG"
image.save(out_buffer, format=fmt)
out_buffer.seek(0)
output_key = f"processed-{key}"
s3.put_object(Bucket=OUTPUT_BUCKET, Key=output_key, Body=out_buffer)
sns.publish(
TopicArn=SNS_TOPIC_ARN,
Message=f"Image {key} processed and saved as {output_key}"
)
print(f"Processed {key}, saved {output_key}")
return {"statusCode": 200, "body": f"Processed {key}"}
