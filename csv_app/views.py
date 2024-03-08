from django.shortcuts import render
from django.http import HttpResponse
from .forms import Csv_form
from .models import Csv_model
import pandas as pd
from sqlalchemy import create_engine
import os
# Create your views here.

def djngo_folder_csv_to_rds():

    try:
        # creating connection
        connection_str = "mysql+pymysql://admin:admin2024@rds-csv-database.crugea0sujs8.ap-south-1.rds.amazonaws.com:3306/mysql_db"

        # creating connection
        connection = create_engine(connection_str)

        # csv collection folder
        data_path = os.path.join(os.getcwd(), "Upload_csv")

        # file names
        file_names = os.listdir(data_path)

        # csv collector
        csv_collection = [file for file in file_names if file.endswith(".csv")]

        # file_paths
        csv_file_paths = [os.path.join(data_path, file) for file in csv_collection]

        # table_names
        table_names = [table_name.split(".")[0] + "_table" for table_name in csv_collection]

        for table_name, file_path in zip(table_names, csv_file_paths):
            # create data frame
            df = pd.read_csv(file_path)

            try:


                # dump to sql
                df.to_sql(table_name, index=False, con=connection)

                print(f"{file_path} dumped in table {table_name} in mysql.")
            except:
                pass

        #for file in csv_file_paths:
            # os.remove(file)

    except Exception as e:
        print(e)


    finally:
        for file in csv_file_paths:
            os.remove(file)
        connection.dispose()


def Csv_view(r):
    form=Csv_form()

    if r.method=='POST':
        form=Csv_form(r.POST,r.FILES)

        if form.is_valid():
            form.save()

            # csv to rds
            djngo_folder_csv_to_rds()

            return HttpResponse('<h1>Successfully file uploded<h1>')

    return render(r,'csv_app/upload_csv.html',{'form':form})


