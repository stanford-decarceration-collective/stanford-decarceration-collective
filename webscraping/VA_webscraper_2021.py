import selenium
from selenium import webdriver
from multiprocessing import Pool
import pandas as pd
from selenium.webdriver.common.by import By
from time import sleep

DOWN = webdriver.common.keys.Keys.ARROW_DOWN
ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
CLEAN_UP = True


class Webpage:

    def __init__(self, done_data: pd.DataFrame):
        self.driver = webdriver.Firefox(executable_path='/Users/pacopoler/Desktop/Recidiviz/geckodriver')
        self.done_data = done_data

    def initialize(self):
        self.driver.get('http://ewsocis1.courts.state.va.us/CJISWeb/circuit.jsp')

    def close(self):
        self.driver.close()

    """ Returns all hearings in a county for people whose names which begin with 'letter' """
    def get_data_by_letter(self, letter, county_name, extra_data=True):
        letter_hearings = pd.DataFrame(columns=self.done_data.columns)
        done_letter_data_len = len(self.done_data[
            (self.done_data.Defendant.apply(lambda x: x[0] == letter)) & (self.done_data.county == county_name)
        ])
        first_row_cn = ''
        start_index = 0

        self._search_for_letter(letter)

        # While there's a next page and we're still on the same letter
        while first_row_cn != self.driver.find_elements(By.TAG_NAME, 'tr')[6].find_element(By.TAG_NAME, 'td').text:
            # range_start will be 0 always except if old scraped data ends in the middle of a page
            range_start = 0
            first_row_cn = self.driver.find_elements(By.TAG_NAME, 'tr')[6].find_element(By.TAG_NAME, 'td').text
            num_table_rows = len(self.driver.find_elements(By.TAG_NAME, 'tr')[6:-2])

            # skip through previously scraped territory
            if start_index < done_letter_data_len:
                # If everything on the page has been scraped, move on
                if start_index + num_table_rows - 1 <= done_letter_data_len:
                    start_index += num_table_rows
                    self.driver.find_element(By.ID, 'nextButton').click()
                    continue

                # Otherwise, skip to the correct place in the table.
                range_start = done_letter_data_len - start_index
                # make start_index bigger than done data index so this condition won't trigger again
                start_index = done_letter_data_len

            for i in range(range_start, num_table_rows):
                row = self.driver.find_elements(By.TAG_NAME, 'tr')[6:-2][i]
                person_data = self._get_person_data(row)
                name = person_data['Name']
                if name[0] != letter:
                    letter_hearings.loc[-1] = letter
                    return letter_hearings

                if extra_data:
                    letter_hearings = letter_hearings.append(self._get_extra_person_data(row), ignore_index=True)
                    # Hit 'Name list' to go back to the list of all names
                    back_button = self.driver.find_elements(By.ID, 'nameList')[0]
                    back_button.click()

                else:
                    letter_hearings = letter_hearings.append(person_data, ignore_index=True)
            self.driver.find_element(By.ID, 'nextButton').click()
        return letter_hearings

    def get_county_hearings_data(self, county_rank, letter):
        county_name = self._pick_county(county_rank)
        letter_data = self.get_data_by_letter(letter, county_name)
        if letter_data.empty:
            completed_flag = pd.DataFrame(columns=self.done_data.columns)
            completed_flag.loc[-1] = letter
            return completed_flag
        letter_data['county'] = county_name
        if set(self.done_data.columns) != set(letter_data.columns):
            pd.set_option('max_columns', 50)
            raise RuntimeError(f'{county_name} data not matching expected: {letter_data.columns}'
                               f'\n {set(letter_data.columns).difference(set(self.done_data.columns))}'
                               f'\n {set(self.done_data.columns).difference(set(letter_data.columns))}')
        letter_data = letter_data[self.done_data.columns]
        letter_data.to_csv(
            'recidiviz/calculator/modeling/population_projection/sdc/webscraping/letter_data_test_13.csv',
            mode='a',
            header=False, # the first time you run this, toggle this to true
        )
        return letter_data

    def _pick_county(self, county_rank):
        county = self.driver.find_elements(By.TAG_NAME, 'option')[county_rank]
        county.click()
        county_name = county.text
        self.driver.find_element(By.ID, 'courtSubmit').click()
        return county_name

    def _search_for_letter(self, letter):
        self.driver.find_element(By.NAME, 'lastName').send_keys(letter)
        self.driver.find_element(By.ID, 'nameSubmit').click()

    def _get_person_data(self, table_row):
        person_data = table_row.find_elements(By.TAG_NAME, 'td')
        return pd.Series({
            'CaseNumber': person_data[0].text,
            'Name': person_data[1].text,
            'Charge': person_data[2].text,
            'HearDate': person_data[3].text,
            'Status': person_data[4].text
        })

    def clean_text(self, text):
        split = text.split(':')
        if len(split) < 2:
            return split[0].strip(), ""
        return split[0].strip(), split[1].strip().replace(',', '[.]')

    def _get_extra_person_data(self, table_row):
        # Click into extended details about person
        table_row.find_element(By.TAG_NAME, 'span').click()

        # Scrape their info
        data_tables = self.driver.find_elements(By.TAG_NAME, 'tbody')
        data_cells = data_tables[4].find_elements(By.TAG_NAME, 'td')[1:-1]
        final_disposition = data_tables[8].find_elements(By.TAG_NAME, 'td')
        details = data_tables[9].find_elements(By.TAG_NAME, 'td')

        case_details_data = [self.clean_text(i.text) for i in data_cells]
        disposition_data = [self.clean_text(final_disposition[i].text) for i in range(6)]
        details_data = [self.clean_text(details[i].text) for i in range(23)]

        return pd.Series(
            {i[0]: i[1] for i in (case_details_data + disposition_data + details_data) if i[0] not in [
                'AKA', 'AKA2', '', 'Restriction Effective Date', 'Restriction End Date'
            ]}
        )


class Scraper:

    county_ranks = pd.Series(pd.read_csv(
        'recidiviz/calculator/modeling/population_projection/sdc/webscraping/county_indices.csv',
        index_col=0
    ).iloc[:, 0])

    def __init__(self):
        self.done_data = pd.concat([
            pd.read_csv(
                f'recidiviz/calculator/modeling/population_projection/sdc/webscraping/letter_data_test_{i}.csv',
                index_col=0
            ) for i in range(13) #change this to be however many letter_data_test files you have
        ])
        # self.done_data = pd.DataFrame() # use this instead of the above the first time you run the script

    def _get_county_records(self, county_rank):
        county_records = pd.DataFrame()

        for letter in ALPHABET:
            # If not in clean-up mode and letter already has data, skip it
            county_name = self.county_ranks[county_rank]
            if (not CLEAN_UP) and self.done_data[
                (self.done_data.Defendant.apply(lambda x: x[0] == letter)) & (self.done_data.county == county_name)
            ].empty:
                continue

            # If in clean_up mode, only continue if letter hasn't uploaded the -1 index "fully done" flag
            if CLEAN_UP and (not self.done_data[
                (self.done_data.Defendant.apply(lambda x: x[0] == letter))
                & (self.done_data.county == county_name)
                & (self.done_data.index == -1)
            ].empty):
                continue

            while True:
                try:
                    print('opening', county_name, letter)
                    browser = Webpage(self.done_data)
                    browser.initialize()
                    t = browser.get_county_hearings_data(county_rank, letter)
                    county_records = pd.concat([county_records, t])
                    break
                except Exception as e:
                    print(f'exception for {county_name}: {e}')
                    browser.close()
            print(f'regular closing {county_name}')
            browser.driver.find_elements(By.TAG_NAME, 'input')[3].click()
            browser.close()
        return county_records

    def get_all_records_parallel(self):
        if __name__ == '__main__':
            processor = Pool(4)
            county_range = list(range(39,120))
            # county_range.remove(34)
            return processor.map(self._get_county_records, county_range)

t = Scraper()
# sleep(3600)
t.get_all_records_parallel()