from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD
import os



class ScrapeFixtures:


    DATE_FORMAT_STRING = "%I:%M%p, %A %d %B %Y"

    def __init__(self):
        pass

    def get_fixtures_links(self, team):
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


    def get_match_details(self, links):

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
                'Kick Off': datetime.strptime(f"{kickoff_datetime[0]}, {kickoff_date[0]} {day} {kickoff_date[2]} {kickoff_date[3]}", self.DATE_FORMAT_STRING),
                'Stadium': game_data.find('span', class_='sdc-site-match-header__detail-venue').text.replace('.', ''),
                'Competition': game_data.find('p', class_='sdc-site-match-header__detail-fixture').text.split('.')[1],
                "SkySportsURL": link
            }  

            matches.append(match)


        return matches


    def save_to_csv(self, matches, team):
        matchesdf = pd.DataFrame(matches)
        matchesdf.to_csv(team.replace(' ', '-').lower() + '-fixtures-upcoming.csv', index=False)
        print(matchesdf)

    def save_to_PostgresDatabase(self, matches):

        try:
            #establishing the connection
            conn = psycopg2.connect(
            database="derbycounty", user='postgres', password=DATABASE_PASSWORD, host='localhost', port= '5432'
            )

            cursor = conn.cursor()

            cursor.execute("SELECT MAX(id) FROM upcoming_fixtures;")

            currentID = cursor.fetchone()[0]+1

            for i in range(len(matches)):

                cursor.execute("INSERT INTO upcoming_fixtures(id, home_team, away_team, kickoff, stadium, competition, sky_sports_url) VALUES(%s, %s, %s, %s, %s, %s, %s)", (currentID, matches.loc[i, "Home Team"], matches.loc[i, "Away Team"], matches.loc[i, "Kick Off"], matches.loc[i, "Stadium"], matches.loc[i, "Competition"], matches.loc[i, "SkySportsURL"]))
                conn.commit()

                currentID += 1

            
            cursor.execute("SELECT * FROM upcoming_fixtures;")

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')



if __name__ == "__main__":

    # teamToFind = input("Which football team do you want to get fixtures for: ")
    webscrapperBot = ScrapeFixtures()

    # matchesList = webscrapperBot.get_match_details(webscrapperBot.get_fixtures_links(teamToFind))
    # webscrapperBot.save_to_csv(matchesList, teamToFind)

    savedMatches = pd.read_csv("derby-county-fixtures-upcoming.csv")

    webscrapperBot.save_to_PostgresDatabase(savedMatches)

