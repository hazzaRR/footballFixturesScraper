from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime


DATE_FORMAT_STRING = "%I:%M%p, %A %d %B %Y"

def get_fixtures_links(team):
    team = team.replace(' ', '-').lower()
    url = f'https://www.skysports.com/{team}-fixtures'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    fixturesLinks = []
    fixtures = soup.find_all('div', class_='fixres__item')

    for fixture in fixtures:

        matchLink = fixture.find('a', class_="matches__item matches__link")
        fixturesLinks.append(matchLink['href'])

    return fixturesLinks


def get_match_details(links):

    matches = []


    for link in links:

        r = requests.get(link)
        soup = BeautifulSoup(r.text, 'html.parser')

        game_data = soup.find('div', class_='sdc-site-match-header__body')

        teams = game_data.find_all('span', class_='sdc-site-match-header__team-name-block-target')
        kickoff_datetime = game_data.find('time', class_='sdc-site-match-header__detail-time').text.split(',')

        kickoff_date = kickoff_datetime[1].replace('.', '').strip()

        kickoff_date = kickoff_date.split(" ")

        day = kickoff_date[1].replace("st", "").replace("nd", "").replace("rd", "").replace("th", "")

        match = {
            'Home Team': teams[0].text,
            'Away Team': teams[1].text,
            'Kick Off': datetime.strptime(f"{kickoff_datetime[0]}, {kickoff_date[0]} {day} {kickoff_date[2]} {kickoff_date[3]}", DATE_FORMAT_STRING),
            'Stadium': game_data.find('span', class_='sdc-site-match-header__detail-venue').text.replace('.', ''),
            'Competition': game_data.find('p', class_='sdc-site-match-header__detail-fixture').text.split('.')[1],
            "SkySportsURL": link
        }  

        matches.append(match)


    return matches


def save_to_csv(matches, team):
    matchesdf = pd.DataFrame(matches)
    matchesdf.to_csv(team.replace(' ', '-').lower() + '-fixtures-upcoming.csv', index=False)
    print(matchesdf)



teamToFind = input("Which football team do you want to get fixtures for: ")
matchesList = get_match_details(get_fixtures_links(teamToFind))

save_to_csv(matchesList, teamToFind)