from django import forms
from .models import Csv_model
class Csv_form(forms.ModelForm):
    class Meta:
        model=Csv_model
        fields='__all__'