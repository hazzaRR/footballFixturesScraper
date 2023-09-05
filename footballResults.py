from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD
import os


DATE_FORMAT_STRING = "%d %b %Y"

def get_all_seasons(team):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    team = team.replace(' ', '-').lower()
    url = f"https://www.11v11.com/teams/{team}/tab/matches/"
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    match_content = soup.find('div', {"id": "pageContent"})

    seasons = match_content.find('ul', class_="linkList").find_all('li')

    for season in seasons:
        seasonLink = season.find('a')
        print(seasonLink['href'])


def get_all_matches(seasonURL):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    team = team.replace(' ', '-').lower()
    url = f"https://www.11v11.com/teams/derby-county/tab/matches/season/{year}/"
    r = requests.get(seasonURL, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    match_content = soup.find('div', {"id": "pageContent"})

    seasons = match_content.find('ul', class_="linkList").find_all('li')

    for season in seasons:
        seasonLink = season.find('a')
        print(seasonLink['href'])

def get_all_matches_using_year(team, year):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
    }
    team = team.replace(' ', '-').lower()
    url = f"https://www.11v11.com/teams/derby-county/tab/matches/season/{year}/"
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    match_content = soup.find('div', class_="column width580")

    season = match_content.find('h2', class_="seasonTitle no-top-margin")

    season = season.text.split(" ")[0]

    table = match_content.find('table', class_="width580 sortable")

    matches = table.find('tbody').find_all('tr')

    data = []

    for match in matches:
        match_data = match.find_all('td')

        date = match_data[0].text
        teams = match_data[1].text.split(" v ")
        result = match_data[2].text
        score = match_data[3].text.strip()

        data.append([datetime.strptime(date, DATE_FORMAT_STRING).date(), teams[0], teams[1], result, score, season])

    matchesdf = pd.DataFrame(data, columns=["Date", "Home Team", "Away Team", "Result", "Score", "Season"])


    matchesdf.to_csv(f"seasonResults/{team.replace(' ', '-').lower()}-match-results-{season}.csv", index=False)

    



    print(matches)
            


if __name__ == "__main__":

    # teamToFind = input("Which football team do you want to get match results for: ")
    # year = input("Which season do you want to get results for i.e 2023/24 = 2024: ")

    # get_match_results(teamToFind, year)
    # get_all_seasons("derby county")

    get_all_matches_using_year("derby county", 1990)



