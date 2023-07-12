#selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import re
import pandas as pd
from urls import url


class scraper():

    def __init__(self,position,location):
        self.position = position
        self.location = location
        self.url = url(position,location)

    service = Service(r"./chromedriver.exe")
    options = webdriver.ChromeOptions()
    chrome_options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--incognito")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service= service, options= options)

    def linkedin_data_1(self):

        self.driver.get(self.url.linkdin_url())

        total_jobs = self.driver.find_element(By.CSS_SELECTOR,"span.results-context-header__job-count")

        total_jobs = int(re.sub('[^0-9]','',total_jobs.text))

        # print(total_jobs)

        if total_jobs > 100:
            total_jobs = 200
        i = 2
        while i <= int(total_jobs/25)+1: 
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            i = i + 1
            # print(i)
            try:
                self.driver.find_element(By.XPATH,'/html/body/div[1]/div/main/section[2]/button').click()
                
                # print('clicked')
                # time.sleep(5)
            except Exception as e:
                # print('passed')
                pass


        all_data = self.driver.find_elements(By.CLASS_NAME,'base-serp-page__content')
        job_lists = all_data[0].find_elements(By.CLASS_NAME,'jobs-search__results-list')
        jobs = job_lists[0].find_elements(By.TAG_NAME,'li')

        job_id= []
        job_title = []
        company_name = []
        location = []
        date = []
        job_link = []
        jd = []
        level = []
    
        for job in jobs:
            job_id0 = job.find_element(By.CSS_SELECTOR,"div").get_attribute("data-row")
            job_id.append(job_id0)
            
            job_title0 = job.find_element(By.CLASS_NAME,"base-search-card__title").text
            job_title.append(job_title0)
            
            company_name0 = job.find_element(By.CLASS_NAME,"base-search-card__subtitle").text
            company_name.append(company_name0)
            
            location0 = job.find_element(By.CLASS_NAME,"job-search-card__location").text
            location.append(location0)
            
            date0 = job.find_element(By.CSS_SELECTOR,"time").get_attribute("datetime")
            date.append(date0)
            
            job_link0 = job.find_element(By.CSS_SELECTOR,"a").get_property("href")
            job_link.append(job_link0)
        
        for i in range(len(job_id)):
            # print(i)
            click_path = f"/html/body/div[1]/div/main/section[2]/ul/li[{i+1}]"
            path_click = job.find_element(By.XPATH,click_path).click()
            time.sleep(3)

            try:
                show_click = "/html/body/div[1]/div/section/div[2]/div/section[1]/div/div/section/button[1]"
                click_it =  self.driver.find_element(By.XPATH,show_click).click()
            except:
                pass

            jd_path = self.driver.find_element(By.CLASS_NAME,"show-more-less-html__markup").text
            jd.append(jd_path)
            
            level_path = self.driver.find_element(By.XPATH,"/html/body/div[1]/div/section/div[2]/div/section[1]/div/ul/li[1]/span").text
            level.append(level_path)

        job_data = pd.DataFrame({"ID": job_id,
                    "Date": date,
                    "Company": company_name, 
                    "Title": job_title,
                    "Experience" : level,
                    "Location": location,
                    "Link": job_link,
                    "JD" : jd
                    })
        
        job_data['JD'] = job_data['JD'].str.replace('\n',' ')

        return job_data.to_csv('./data/job_data')         
        

    # def indeed_data(self):

    #     self.driver.get(self.url.indeed_url())

    #     total_jobs = self.driver.find_element(By.CLASS_NAME,"jobsearch-JobCountAndSortPane-jobCount")

    #     total_jobs = int(re.sub('[^0-9]','',total_jobs.text))

    #     print(total_jobs)

    #     # print(total_jobs)
    #     if total_jobs > 100:
    #         total_jobs = 100
    #     i = 2
    #     # while i <= int(total_jobs/17)+1: 
            
    #     job_lists = self.driver.find_element(By.CLASS_NAME,"jobsearch-LeftPane")
    #     jobs = job_lists[0].find_elements(By.TAG_NAME,'li')

    #     print(job_lists)

        

sc = scraper("Data Scientist","Hyderabad")
sc.linkedin_data_1()