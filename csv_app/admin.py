from django.contrib import admin
from .models import Csv_model

# Register your models here.
class Csv_admin(admin.ModelAdmin):
    pass
admin.site.register(Csv_model,Csv_admin)