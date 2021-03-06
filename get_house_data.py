import requests
from time import sleep, time
from tqdm import tqdm
from bs4 import BeautifulSoup 
import pandas as pd

ROOT_URL = 'http://www.vanfun.net'

def only_number(x):
    x = re.findall('\d+', x)
    x = int(x[0])+int(x[1])/100
    return x

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
    
def get_house_detail_to_df(dataframe):
    # container
    ids = []
    temp_info = pd.DataFrame()
    
    for ide in tqdm(dataframe.id.values):
        req = requests.get(ROOT_URL+'/house-{}.aspx'.format(int(ide)))
        temp_soup = BeautifulSoup(req.text, "lxml")
        if temp_soup.find('tbody') is None:
            pass       
        else:
            # do stuff here
            house_info_table = temp_soup.find('tbody')
            # add record to dataframe
            house_info_table = pd.DataFrame({'MLS_number': house_info_table.find('input',{'class':'house_mslno'})['value'],
                             'listed_date': house_info_table.find('td',title=True).text,
                             'house_size':house_info_table.find_all('span',{'class':'numb area'})[0].text,
                             'land_size':house_info_table.find_all('span',{'class':'numb area'})[1].text ,
                             'feature':house_info_table.findAll('td',attrs={'class': None})[3].text,
                             'land_tax':house_info_table.findAll('td',attrs={'class': None})[5].text
                            }, index=[0])
            # add id
            ids.append(ide)
            temp_info = pd.concat([temp_info, house_info_table])
            sleep(1)
     # add ids to dataframe
    temp_info['id'] = ids
    temp_info = temp_info.reset_index(drop=True)     
    return temp_info

def parallelize_dataframe(df, func):
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
    
def get_coord(dataframe):
    ids = []
    coords = []
    
    for row in tqdm(dataframe[['id','addresses']].iterrows()):
        try:
            location = geolocator.geocode(row[1].addresses)
            if location is None:
                ids.append(row[1].id)
                coords.append('Not Found')
            else:
                ids.append(row[1].id)
                coords.append((location.latitude, location.longitude))
            sleep(1)
        except GeocoderTimedOut as e:
            print("Error: geocode failed on input %s with message %s"%(row[1].addresses, e.message))
            pass
    coords_df = pd.DataFrame({'ids':ids,
                             'coords':coords})
    return coords_df
  
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
    total_detail = parallelize_dataframe(total_vancouver,get_house_detail_to_df)
    total_detail.to_excel('total_detail.xlsx')
    
    total_vancouver = total_vancouver.merge(total_detail,on='id').reset_index(drop=True)
    total_vancouver.to_excel('final.xlsx')
    
    ## add coordinates 
    coordinates = parallelize_dataframe(total_vancouver,get_coord)
    total_vancouver = total_vancouver.merge(coordinates,on='id').reset_index(drop=True)
    
if __name__ == "__main__":
    main()
