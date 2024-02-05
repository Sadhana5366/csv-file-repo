from django.db import models

# Create your models here.
class Csv_model(models.Model):
    name=models.CharField(max_length=50)
    email=models.EmailField()
    file=models.FileField(upload_to='Upload_csv')