from bs4 import BeautifulSoup
import requests
import csv
from multiprocessing import Pool,current_process
import sqlite3 as sql

conn = sql.connect('technodomFull.db')
cursor = conn.cursor()		


def get_html(url):
	r = requests.get(url)
	return r.text




def get_total_link(html):
	soup = BeautifulSoup(html,'lxml')

	s_t = soup.find('ul',class_="top-menu").find_all('li',class_="top-menu__item")

	items_link = []
	for menu in s_t:
		
		inner_menu = menu.find('div',class_='inner-menu').find_all(lambda tag : tag.name == 'li' and tag.get('class') == ['inner-menu__item'] )
		
		

		for item in inner_menu:

			items_link.append('https://www.technodom.kz' + item.find('a').get('href'))

	return items_link


def get_total_pages(html):
	try:
		soup = BeautifulSoup(html,'lxml')

		total_pages = soup.find('ul',class_='pagination').find_all('li',class_='pagination-item')[-2].text.strip()

		return int(total_pages)
	except:
		return 0




def get_page_data(html):
	soup = BeautifulSoup(html,'lxml')

	ads = soup.find('div',class_='productGrid').find_all('div',class_='tda-product-grid__item')

	for ad in ads:
		

		
		try:
			name = ad.find('div',class_='basetile__topside').find('div',class_='basetile__wrapper').find('a',class_='basetile__title').text
		except:
			name = ''
		

		
		try:
			price = ad.find('div',class_= 'basetile__bottomside').find('div',class_='basetile__price').text.strip().split(' ')[0]
		except:
			price = ''
		

		
		try :
			by_month_price = ad.find('div',class_='basetile__bottomside').find('div',class_='basetile__specbuy tda-product-grid__item__h-elem').find('div').text
			array = by_month_price.split(' ')[1].split('\xa0')
			price_month = array[0]+array[1]
		
		except:
			price_month = ''
		

		
		try:
			url = 'https://www.technodom.kz' + ad.find('div',class_='basetile__topside').find('a').get('href')
		except:
			url = ''

		data =     {'name' :name,
					'price':price,
					'by_month_price':price_month,
					'url':url}

		write_to_database(data)





#def write_csv(data):
#	with open('technodomFull.csv','a',encoding = 'utf-8') as f:
#		writer = csv.writer(f)
#
#		writer.writerow( (data['name'],
#						 data['price'],
#						 data['by_month_price'],
#						 data['url'])  )

def write_to_database(data):
	cursor.execute("INSERT INTO technodomFull (:name,:price,:price_by_month,:url)",{'name' : data['name'],'price' : data['price'],'price_by_month' : data['price_by_month'],'url':data['url']})
	

def make_all(url):
	
	
	html = get_html(url)
	
	
	get_page_data(html)
		


def main():
	url = "https://www.technodom.kz"
	
	all_links = get_total_link(get_html(url))
	total_links = []


	for index,link in enumerate(all_links):
		
		total_pages = get_total_pages(get_html(all_links[index]))
		
		url_gen = all_links[index]

		for i in range(0,total_pages):
			if i >= 1:
				url_gen = all_links[index] + '/page/' + str(i)
		
			total_links.append(url_gen)
	

	with Pool(50) as p :
		p.map(make_all,total_links)
	
		


	


if __name__ == '__main__':
	main()  
	conn.close()