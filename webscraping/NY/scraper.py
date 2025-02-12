from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By

options = Options()
options.binary_location = 'C:/Program Files/Mozilla Firefox/firefox.exe'
service = Service(executable_path='C:/Users/isaac/Code/python/geckodriver/geckodriver.exe')
driver = webdriver.Firefox(service=service, options=options)

button_xpath = '/html/body/app/div[1]/div/div[3]/div[3]/form/div[2]/div[2]/div/button[1]'
name_xpath = '/html/body/app/div[1]/div/div[3]/div[4]/div[2]/div[1]/div/div[1]/div/h3'

def check_id(din):
    driver.get('https://nysdoccslookup.doccs.ny.gov/')
    driver.implicitly_wait(10)

    din_input = driver.find_element(By.ID, 'din')
    button = driver.find_element(By.XPATH, button_xpath)
    din_input.send_keys(din)
    button.click()

    name = driver.find_element(By.XPATH, name_xpath)
    ethnicity = driver.find_element(By.XPATH, ethnicity_xpath)
    dob = driver.find_element(By.XPATH, dob_xpath)
    age = driver.find_element(By.XPATH, age_xpath)
    custody_status = driver.find_element(By.XPATH, custody_status_xpath)
    date_received = driver.find_element(By.XPATH, date_received_xpath)
    date_received_current = driver.find_element(By.XPATH, date_received_current_xpath)

    print('Found name!', name.text)

check_id('22R1495')
#check_id('22R1496')
#driver.quit()
