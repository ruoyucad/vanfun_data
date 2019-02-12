import requests
from time import sleep, time
from tqdm import tqdm
from bs4 import BeautifulSoup 
import pandas as pd

ROOT_URL = 'http://www.vanfun.net'

def get_listing_data(type_code, endpage):
    '''
    type_code 1 = house
    type_code 2 = townhouse
    type_code 3 = apartment
    '''
    all_page_response = []
    for page in tqdm(range(1,endpage)): 
        req = requests.get(ROOT_URL+'/house-{}-0-0-0-0-0-0-0-0-0-{}-0-0-0-0-0-0-0-0.aspx'.format(type_code,page))
        temp_soup = BeautifulSoup(req.text, "lxml")
        # 从第三个开始 前面两个是广告
        listing_div_list = temp_soup.find('dl',{"id":"goodsList"}).find_all("dd",title =True)[3:]
        all_page_response.append(listing_div_list)
        sleep(1)
    return all_page_response


def get_info_per_page(listing_page):
    temp_page = pd.DataFrame()
    for house in listing_page:
        temp = pd.DataFrame({'address': house["title"],
                             'id': house.find_all('a')[0]['data-code'],
                             'house__detail_link':ROOT_URL+house.find_all('a')[0]['href'],
                             'house_config':house.find('span',{"class":"orange"}).text,
                             'list_comapny':house.find('span',{"class":"listingof"}).text,
                             'price_cad':house.find('span',{"class":"price"}).text,
                             'price_rmb':house.find('span',{"class":"price_rmb"}).text
                            }, index=[0])

        temp_page = pd.concat([temp_page, temp])
    return temp_page


def save_to_excel(type_code,endpage, reponses):

    total_listing = pd.DataFrame()
    chunk_of_df = []
    for page in tqdm(range(endpage-1)):
        chunk_of_df.append(get_info_per_page(reponses[page]))
    total_listing = pd.concat(chunk_of_df,ignore_index=True)
    total_listing.to_excel('total_vancouver_{}_data.xlsx'.format(type_code))
    return total_listing
    
def get_house_detail(dataframe):
    all_house_response = []
    ids = []
    for ide in tqdm(dataframe.id.values):
        req = requests.get(ROOT_URL+'/house-{}.aspx'.format(int(ide)))
        temp_soup = BeautifulSoup(req.text, "lxml")
        house_info_table = temp_soup.find('table',{"class":"houseTable"}).find('tbody')
        all_house_response.append(house_info_table)
        ids.append(ide)
    return all_house_response, ids

def get_info_per_house(house_reponse, ids):
    temp_info = pd.DataFrame()
    for house in house_reponse:
        temp = pd.DataFrame({'MLS_number': house.find('input',{'class':'house_mslno'})['value'],
                             'listed_date': house.find('td',title=True).text,
                             'house_size':house.find_all('span',{'class':'numb area'})[0].text,
                             'land_size':house.find_all('span',{'class':'numb area'})[1].text ,
                             'feature':house.findAll('td',attrs={'class': None})[3].text,
                             'land_tax':house.findAll('td',attrs={'class': None})[5].text
                            }, index=[0])

        temp_info = pd.concat([temp_info, temp])
    temp_info['id'] = ids
    temp_info = temp_info.reset_index(drop=True)
    return temp_info
    

def main():
    # get house data
    house_data = get_listing_data(2,692)
    house = save_to_excel(2,692, house_data)
    # get townhouse data
    townhouse_data = get_listing_data(2,267)
    townhouse = save_to_excel(2,267, townhouse_data)
    # get apartment data
    apartment_data = get_listing_data(3,542)
    apartment = save_to_excel(3,542, apartment_data)
    # total data
    total_vancouver = pd.concat([house,townhouse,apartment]).reset_index(drop=True)
    
    # get house detail
    all_house_response, ids = get_house_detail(total_vancouver)
    house_info = get_info_per_house(all_house_response,ids)
    
    # merge two dataframe together
    total_vancouver = total_vancouver.merge(house_info, on='id')
    
if __name__ == "__main__":
    main()