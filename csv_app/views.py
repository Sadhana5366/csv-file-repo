import boto3
from django.shortcuts import render
from django.http import HttpResponse
from .forms import Csv_form
from .models import Csv_model
from django.core.mail import send_mail
from django.conf import settings


# Create your views here.
def Csv_view(r):
    form = Csv_form()

    if r.method == 'POST':
        form = Csv_form(r.POST, r.FILES)

        if form.is_valid():
            file_instance = form.save()

            # Subscribe the email to the SNS topic
            email_address = form.cleaned_data['email']
            sns_topic_arn = 'arn:aws:sns:ap-south-1:165860449756:object-notification'
            subscribe_email_to_sns(email_address, sns_topic_arn)

            # Send a notification email to the person
            send_notification_email(email_address, file_instance)

            return HttpResponse('<h1>Successfully file uploded<h1>')

    return render(r, 'csv_app/upload_csv.html', {'form': form})


def subscribe_email_to_sns(email_address, sns_topic_arn):
    try:
        client = boto3.client('sns', region_name='ap-south-1')  # Replace 'your-region' with the appropriate AWS region
        response = client.subscribe(
            TopicArn=sns_topic_arn,
            Protocol='email',
            Endpoint=email_address
        )

        # Print the subscription ARN for verification (optional)
        print("Subscription ARN:", response['SubscriptionArn'])

    except Exception as e:
        print(f"Error subscribing email address: {str(e)}")
        # Handle the error, log it, or provide feedback to the user



def send_notification_email(email_address, file_instance):
    subject = 'File Upload Notification'
    message = f'Thank you for uploading the file {file_instance.file.name}.'
    from_email = settings.DEFAULT_FROM_EMAIL
    recipient_list = [email_address]
    
    send_mail(subject, message, from_email, recipient_list)
