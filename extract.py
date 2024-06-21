from bs4 import BeautifulSoup
import pandas as pd
import requests

res = requests.get('https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity#Football_stadiums_by_capacity')

soup = BeautifulSoup(res.text, 'html.parser')

table = soup.find_all("table", class_= "wikitable sortable sticky-header")

#print(len(table))

#print(table)

table_rows = table[0].find_all('tr')

header = [col_name.getText().replace('\n', '') for col_name in table_rows[0].find_all("th")][1:]



data = [[val.getText()for val in row.find_all("td")] for row in table_rows[1:]]

print(data[0])
pd.DataFrame(data= data, columns= header).to_csv('stads.csv', index= False)
