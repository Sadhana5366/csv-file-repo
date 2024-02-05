from django.shortcuts import render
from django.http import HttpResponse
from .forms import Csv_form
from .models import Csv_model
# Create your views here.
def Csv_view(r):
    form=Csv_form()

    if r.method=='POST':
        form=Csv_form(r.POST,r.FILES)

        if form.is_valid():
            form.save()
            return HttpResponse('<h1>Successfully file uploded<h1>')

    return render(r,'csv_app/upload_csv.html',{'form':form})

