#Import Libraries

import requests as r
from bs4 import BeautifulSoup as bs
import pandas as pd
from log import log_into_file
import time

#The numbers of pages which I want to extract data
max_pages = 2

#Getting Job Titles 
def get_title() :
    
    df = pd.DataFrame(columns = ["title"])
    elem = []
    for i in range(1,max_pages):

        url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
        request = r.get(url)
        soup = bs(request.text, "html.parser")
        job_titles = soup.find_all("div", {"class" : "job-card_jobCard__body__86jgk card-body"})

        for job in job_titles :
            elem.append(job.a.text)
        
    for i, job in enumerate(elem):
        df.loc[i] = job
        
    return df
    
    #THE BELOW CODE SORTED AUTOMATICALLY THE DATAFRAME
    # for elem in job_titles :
    #     data_dict = {"Title" : elem.a.text}
    #     df1 = pd.DataFrame(data_dict, index=[0])
    #     df = pd.concat([df1,df], ignore_index=True)
    
#Getting Job Links 
def get_links() :
    
    df = pd.DataFrame(columns = ["link"])
    for i in range(1,max_pages):
        
        url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
        request = r.get(url)
        soup = bs(request.text, "html.parser")
    
        links = soup.find_all("h2", {"class" : "job-card_jobResultHeading__title__IQ8iT"})
            
        for elem in links :
            a = elem.find("a")
            href = a.get("href")
            data_dict = {"link" : "https://www.reed.co.uk" + href}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
        
    return df

#Getting Job Salary
def get_salary() :
    
    df = pd.DataFrame(columns = ["salary"])
    for i in range(1,max_pages):

        url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
        
        request = r.get(url)
        soup = bs(request.text, "html.parser")
        job_salary  = soup.find_all("li", {"class" : "job-card_jobMetadata__item___QNud list-group-item"})
        
        for elem in job_salary :
            if elem.find("svg" , {"aria-labelledby" : "title-salary"}) :
                data_dict = {"salary" : elem.text}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df

#Getting Job Types 
def get_job_type() :
    
    df = pd.DataFrame(columns = ["job_type"])
    for i in range(1,max_pages):

        url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
        
        request = r.get(url)
        soup = bs(request.text, "html.parser")
        job_salary  = soup.find_all("li", {"class" : "job-card_jobMetadata__item___QNud list-group-item"})
        
        for elem in job_salary :
            if elem.find("svg" , {"aria-labelledby" : "title-clock"}) :
                data_dict = {"job_type" : elem.text}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df

#Getting Job Location
def get_location() :
    
    df = pd.DataFrame(columns = ["location"])
    for i in range(1,max_pages):

        url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
        
        request = r.get(url)
        soup = bs(request.text, "html.parser")
        job_salary  = soup.find_all("li", {"class" : "job-card_jobMetadata__item___QNud list-group-item"})
        
        for elem in job_salary :
            if elem.find("svg" , {"aria-labelledby" : "title-location"}) :
                data_dict = {"location" : elem.text}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    
    return df

#Getting Company Name
def get_company_name() :
    
    df = pd.DataFrame(columns = ["company_name"])
    for i in range(1,max_pages):

        url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
        
        request = r.get(url)
        soup = bs(request.text, "html.parser")
        company_name  = soup.find_all("div", {"class" : "job-card_jobResultHeading__postedBy__sK_25"})
        
        for name in company_name :
            data_dict = {"company_name" : name.find("a").text}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df
    
def extract() :  
    try:
        log_into_file("info", "Extracting Data ...\n")
        df_results = pd.DataFrame()

        df_results = pd.concat([get_title(), get_company_name(), get_salary(), get_location(), get_job_type(), get_links()], axis = 1, ignore_index=True)
        
        log_into_file("info",  "Loading Data ...\n")
        time.sleep(3)
        log_into_file("info",  "Extraction Completed\n")
        time.sleep(3)
    except Exception as e:
        log_into_file("error",e)

    df_results.columns = ["title", "company", "salary", "location", "job_type", "link"]
    
    return df_results
