#IMPORT LIBRARIES
import requests as r 
from bs4 import BeautifulSoup as bs
import pandas as pd
from log import log_into_file

max_pages = 5

def extract() :
    
    try:
        df_results = pd.DataFrame(columns = ["title","company","salary","location","job_type","link"])
        
        pd_title = pd.DataFrame() 
        pd_company = pd.DataFrame() 
        pd_salary = pd.DataFrame() 
        pd_location = pd.DataFrame() 
        pd_job_type = pd.DataFrame()
        pd_link = pd.DataFrame()
        
        for i in range(1,max_pages) :
            url = f"https://www.reed.co.uk/jobs/data-engineer-jobs?pageno={i}"
            request = r.get(url)
            soup = bs(request.text, "html.parser")
            
            job_titles = soup.find_all("div", {"class" : "job-card_jobCard__body__86jgk card-body"})
            links = soup.find_all("h2", {"class" : "job-card_jobResultHeading__title__IQ8iT"})
            job_salary  = soup.find_all("li", {"class" : "job-card_jobMetadata__item___QNud list-group-item"})
            job_type  = soup.find_all("li", {"class" : "job-card_jobMetadata__item___QNud list-group-item"})
            job_location  = soup.find_all("li", {"class" : "job-card_jobMetadata__item___QNud list-group-item"})
            company_name  = soup.find_all("div", {"class" : "job-card_jobResultHeading__postedBy__sK_25"})
            
            for titles in job_titles :
                data = {"title" : titles.a.text}
                df = pd.DataFrame(data, index = [0])
                pd_title = pd.concat([pd_title, df], ignore_index = True)
                
            for companies in company_name : 
                data = {"company" : companies.find("a").text}
                df = pd.DataFrame(data, index = [0])
                pd_company = pd.concat([pd_company, df], ignore_index = True)
                
            for location in job_location : 
                if location.find("svg" , {"aria-labelledby" : "title-location"}) :
                    data = {"location" : location.text}
                    df = pd.DataFrame(data, index = [0])
                    pd_location = pd.concat([pd_location, df], ignore_index = True)
            
            for job_typ in job_type : 
                if job_typ.find("svg" , {"aria-labelledby" : "title-clock"}) :
                    data = {"job_type" : job_typ.text}
                    df = pd.DataFrame(data, index = [0])
                    pd_job_type = pd.concat([pd_job_type, df], ignore_index = True)
            
            for salary in job_salary : 
                if salary.find("svg" , {"aria-labelledby" : "title-salary"}) :
                    data = {"job_salary" : salary.text}
                    df = pd.DataFrame(data, index = [0])
                    pd_salary = pd.concat([pd_salary, df], ignore_index = True)
            
            for link in links :
                a = link.find("a")
                href = a.get("href")
                data = {"link" : "https://www.reed.co.uk" + href}
                df = pd.DataFrame(data, index = [0])
                pd_link = pd.concat([pd_link, df], ignore_index = True)
            
        df_results = pd.concat([pd_title,pd_company,pd_salary,pd_location,pd_job_type, pd_link], axis = 1, ignore_index = True)
        
        log_into_file("info", "Extracting Data ...\n")
        log_into_file("info",  "Loading Data ...\n")
        log_into_file("info",  "Extraction Completed\n")
    except Exception as e:
        log_into_file("error",e)
        
    df_results.columns = ["title", "company", "salary", "location", "job_type", "link"]
    
    return df_results
