import csv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

options = Options()
options.binary_location = 'C:/Program Files/Mozilla Firefox/firefox.exe'
service = Service(executable_path='C:/Users/isaac/Code/python/geckodriver/geckodriver.exe')
driver = webdriver.Firefox(service=service, options=options)

button_xpath = '/html/body/app/div[1]/div/div[3]/div[3]/form/div[2]/div[2]/div/button[1]'
error_xpath = '/html/body/app/div[1]/div/div[3]/div[1]'
new_search_xpath = '/html/body/app/div[1]/div/div[3]/div[2]'

record_xpaths = {
    'name': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[1]/div/h3',
    'ethnicity': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[4]/div[1]',
    'age': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[4]/div[3]',
    'custody_status': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[5]/div[2]',
    'date_received': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[8]/div[2]',
    'date_received_current': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[9]/div[2]',
    'crimes_class': lambda i: f'/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[4]/div/table/tbody/tr[{i}]/td[1]/h5',
    'crimes_name': lambda i: f'/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[4]/div/table/tbody/tr[{i}]/td[2]/h5',
    'aggregate_min_sentence': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[7]/div[2]',
    'aggregate_max_sentence': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[8]/div[2]',
    'earliest_release_date': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[9]/div[2]',
    'earliest_release_type': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[10]/div[2]',
    'parole_interview_date': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[11]/div[2]',
    'parole_interview_type': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[12]/div[2]',
    'conditional_release_date': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[14]/div[2]',
    'max_expiration_date': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[15]/div[2]',
    'max_expiration_date_parole_supervision': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[16]/div[2]',
    'post_release_supervision_max_expiration_date': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[17]/div[2]',
    'parole_board_discharge_date': '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[18]/div[2]'
}

def check_id(din):
    try:
        din_input = driver.find_element(By.ID, 'din')
        button = driver.find_element(By.XPATH, button_xpath)

        din_input.send_keys(din)
        button.click()

        record = {}

        for key in record_xpaths:
            if key == 'crimes_class':
                record['crimes_name'] = ''
                record['crimes_class'] = ''
                
                for i in range(1, 4):
                    crime_name = driver.find_element(By.XPATH, record_xpaths['crimes_name'](i)).text
                    crime_class = driver.find_element(By.XPATH, record_xpaths['crimes_class'](i)).text

                    if crime_name.strip() == '':
                        break

                    record['crimes_name'] += crime_name + ','
                    record['crimes_class'] += crime_class + ','

            elif key == 'crimes_name':
                pass
            elif type(key) == str:
                record[key] = driver.find_element(By.XPATH, record_xpaths[key]).text
        return record
    
    except NoSuchElementException:
        try:
            error = driver.find_element(By.XPATH, error_xpath)
            print('Error:', error.text)
            record = False
        except NoSuchElementException:
            # Keep waiting if there's no error
            record = check_id(din)
    except Exception:
        record = False
    finally:
        if record:
            WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, new_search_xpath))).click()
        else:
            din_input.clear()

        return record

with open('ny_records.csv', 'w') as records:
    writer = csv.writer(records, lineterminator='\n')
    writer.writerow(list(record_xpaths.keys()))

    year = 22
    reception_faciltiies = ['A']#, 'B', 'C', 'G','R']
    inmate_no = 0

    driver.get('https://nysdoccslookup.doccs.ny.gov/')
    driver.implicitly_wait(2)
        

    for facility in reception_faciltiies:
        for i in range(1, 100):
            #driver.get('https://nysdoccslookup.doccs.ny.gov/')
            din = f'{year}{facility}{int(i):04d}'
            record = check_id(din)
            if record:
                writer.writerow(list(record.values()))

            #WebDriverWait(driver, 1000000).until(EC.element_to_be_clickable((By.XPATH, new_search_xpath))).click()
            #new_search = driver.find_element(By.XPATH, new_search_xpath)
            #new_search.click()

driver.quit()
