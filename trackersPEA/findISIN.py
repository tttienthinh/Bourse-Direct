from selenium import webdriver 
from bs4 import BeautifulSoup

"""
Ce script s'éxecute ligne par ligne dans le terminal Python pour pouvoir éxecuter add_isin au bon moment.
A chaque fois tous les ISIN de la page ajouté (très rapide), on peut changer de page et réexécuter add_isin.
"""

# Driver
driver = webdriver.Chrome()
driver.get("https://www.boursedirect.fr/fr/marches/recherche?nature=tracker&pea=true")

# Fonction
def add_isin(page_source):
    soup = BeautifulSoup(page_source, 'lxml')
    isin_list = soup.find_all('span', {"class":'isin'})
    with open("trackersPEA/ISIN.txt", "a+") as file:
        for isin in isin_list:
            file.write(isin.text + "\n")
    file.close()

# Exécution
add_isin(page_source = driver.page_source)

# Fin
driver.close()