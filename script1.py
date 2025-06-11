import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# Chemin vers ChromeDriver
CHROMEDRIVER_PATH = "chromedriver.exe"

# Liste des banques et villes à scraper
BANQUES = [
    "CIH Bank", "Attijariwafa Bank", "BMCE Bank", "Banque Populaire",
    "BMCI", "Crédit Agricole du Maroc", "Société Générale Maroc",
    "Banque Centrale Populaire", "Bank of Africa", "Al Barid Bank",
    "Crédit du Maroc", "Bank Al-Maghrib", "Bank Assafa", "Umnia Bank"
]

VILLES = [
    "Casablanca", "Rabat", "Marrakech", "Fès", "Tanger", "Agadir",
    "Oujda", "Meknès", "Tétouan", "Kenitra", "Safi", "El Jadida",
    "Beni Mellal", "Errachidia", "Nador", "Settat", "Larache",
    "Khouribga", "Guelmim", "Laâyoune", "Dakhla", "Ouarzazate",
    "Mohammedia", "Taza", "Taourirt", "Ksar El Kebir", "Berkane",
    "Sidi Bennour", "Essaouira", "Taroudant", "Tiznit", "Azilal"
]



options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

# Stockage des résultats
resultats = []
agences_scrapees = set()

def scroll_to_load_all_agences():
    try:
        last_height = driver.execute_script("return document.querySelector('.m6QErb').scrollHeight")
        while True:
            driver.execute_script("document.querySelector('.m6QErb').scrollTo(0, document.querySelector('.m6QErb').scrollHeight);")
            time.sleep(random.uniform(2, 4))
            new_height = driver.execute_script("return document.querySelector('.m6QErb').scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    except:
        print("Fin du scrolling des agences.")

def chercher_agences(nom_banque, ville):
    print(f"\n Recherche de toutes les agences de {nom_banque} à {ville}...")
    
    driver.get("https://www.google.com/maps")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "q")))

    search_box = driver.find_element(By.NAME, "q")
    search_box.send_keys(f"{nom_banque} {ville}")
    search_box.send_keys(Keys.RETURN)

    try:
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "hfpxzc")))

    except:
        print(f"⚠️ Aucune agence trouvée pour {nom_banque} à {ville}")
        return []

    scroll_to_load_all_agences()

    agences = driver.find_elements(By.CLASS_NAME, "hfpxzc")
    liens_agences = [agence.get_attribute("href") for agence in agences if agence.get_attribute("href")]

    print(f" {len(liens_agences)} agences trouvées pour {nom_banque} à {ville}.")
    return liens_agences

def cliquer_plus_davis():
    """Clique sur 'Plus d'avis' si disponible"""
    try:
        while True:
            bouton = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "w8nwRe"))
            )
            bouton.click()
            time.sleep(2)
    except:
        print(" Tous les avis visibles ont été chargés.")

def charger_tous_les_avis():
    try:
        cliquer_plus_davis()
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print("🔄 Aucun nouvel avis chargé, arrêt du scrolling.")
                break
            last_height = new_height
    except Exception as e:
        print(" Erreur lors du scrolling :", e)

def extraire_infos_agence():
    """Extrait les informations d'une agence"""
    try:
        wait = WebDriverWait(driver, 15)

        # Extraction du nom de l'agence
        try:
            nom = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))).text
        except:
            nom = "Nom inconnu"

        # Extraction de l'adresse/localisation
        try:
            localisation = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "Io6YTe"))).text
        except:
            localisation = "Localisation inconnue"

        # Extraction de la note
        try:
            note = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "fontDisplayLarge"))).text
        except:
            note = "0"

        print(f" Nom : {nom},  Localisation : {localisation},  Note : {note}")
        return nom, localisation, note
    except Exception as e:
        print(f"⚠️ Erreur lors de l'extraction des informations de l'agence : {e}")
        return "Inconnu", "Inconnu", "0"

def extraire_avis():
    """Extrait les avis visibles"""
    charger_tous_les_avis()
    avis_data = []
    try:
        avis_elements = driver.find_elements(By.CLASS_NAME, "jftiEf")
        for avis in avis_elements:
            try:
                texte = avis.find_element(By.CLASS_NAME, "wiI7pd").text
                date = avis.find_element(By.CLASS_NAME, "rsqaWe").text

                # Vérifier si l'avis est en arabe
                if any("\u0600" <= char <= "\u06FF" for char in texte):
                    langue = "Arabe"
                else:
                    langue = "Français"

                avis_data.append((texte, date, langue))
                print(f" Date : {date}\n Avis ({langue}) : {texte}\n" + "-"*50)
            except:
                continue
    except Exception as e:
        print("⚠️ Erreur lors de l'extraction des avis :", e)
    return avis_data

# Lancer le scraping
for banque in BANQUES:
    for ville in VILLES:
        liens = chercher_agences(banque, ville)
        for lien in liens:
            if lien in agences_scrapees:
                continue
            agences_scrapees.add(lien)

            driver.get(lien)
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf")))

            except:
                print(f"⚠️ Impossible de charger la page de l'agence {lien}")
                continue

            nom, localisation, note = extraire_infos_agence()
            avis = extraire_avis()

            # Enregistrement dans la liste des résultats
            for texte, date, langue in avis:
                resultats.append([banque, ville, nom, localisation, note, texte, date])

            print(f"✅ {nom} - {len(avis)} avis enregistrés.\n")
            time.sleep(random.uniform(2, 5))


df = pd.DataFrame(resultats, columns=["Banque", "Ville", "Nom Agence", "Localisation", "Note", "Avis", "Date Avis"])
df.to_csv("donnees_agences_avis.csv", index=False, encoding="utf-8-sig", sep=";")

print("\nDonnées enregistrées ")

driver.quit()