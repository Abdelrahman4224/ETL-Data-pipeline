import glob                         # this module helps in selecting files and batch file processing
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime       # date and time module

#download the file
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip
#unzip the files
!unzip datasource.zip -d dealership_data      



#start of the extract process
#create the files needed
tmpfile    = "dealership_temp.tmp"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored


# CSV extract function.
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe
	
	
# JSON extract function.
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe
	
	
# XML extract function. unlike json and csv we can't use pandas, xml needs processing with etree module
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel'])
    tree = ET.parse(file_to_process) #process the file
    root = tree.getroot()  #get the root
    for person in root:    #loop through each person element
        car_model = person.find("car_model").text
        year_of_manufacture = int(person.find("year_of_manufacture").text)
        price = float(person.find("price").text)
        fuel = person.find("fuel").text
        dataframe = dataframe.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}, ignore_index=True)
    return dataframe
	
	
#creat an extract function using above functions to process all files and return a complete datafram with all the data
def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel']) # create an empty data frame to hold extracted data
    
    #process all csv files
    for csvfile in glob.glob("dealership_data/*.csv"): #use glob to get all files with same format we give the files path as argument
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)   #append the data and we ignore index so the original index in not appended in the new datafram
        
    #process all json files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data
	

#start of the transform process , creat a simple transform function that will round price column 2 decimal places
def transform(data):
     data["price"] = round(data.price,2)
     return data
	 

#start of the loading process
#creat a load to csv file function using pandas
def load(file,data):
    data.to_csv(file) 
	

#finally create a logging function to document the process
def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')




# Creat the body ans start the ETL Process
log("ETL Started")

log("Extract Started")

data = extract()

log("Extract Finished")

log("Transform Started")

data = transform(data)

log("Transform Finished")

log("Load Started")

load(targetfile,data)

log("Load Finished")

log("ETL Finished")		
