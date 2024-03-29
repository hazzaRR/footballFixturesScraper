from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD, DATABASE_HOST, DATABASE_USERNAME
import logging



class ScrapeFixtures:

    DATE_FORMAT_STRING = "%I:%M%p, %A %d %B %Y"

    def __init__(self):
        pass

    def get_fixtures_links(self, team):
        headers = {  
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
            'Cache-Control': 'no-cache'
        }
        team = team.replace(' ', '-').lower()
        url = f'https://www.skysports.com/{team}-fixtures'
        r = requests.get(url, headers=headers)
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
            database="derbycounty", user=DATABASE_USERNAME, password=DATABASE_PASSWORD, host=DATABASE_HOST, port= '5432'
            )

            cursor = conn.cursor()

            cursor.execute("SELECT MAX(id) FROM upcoming_fixtures;")

            currentID = cursor.fetchone()[0]

            if (currentID == None):
                currentID = 1
            else:
                currentID += 1

            for i in range(len(matches)):

                try:

                    cursor.execute("INSERT INTO upcoming_fixtures(id, home_team, away_team, kickoff, stadium, competition, sky_sports_url) VALUES(%s, %s, %s, %s, %s, %s, %s)", (currentID, matches.loc[i, "Home Team"], matches.loc[i, "Away Team"], matches.loc[i, "Kick Off"], matches.loc[i, "Stadium"], matches.loc[i, "Competition"], matches.loc[i, "SkySportsURL"]))
                    conn.commit()

                    currentID += 1

                except psycopg2.IntegrityError as e:
                    if "duplicate key" in str(e):
                        print("Match fixture already exists.. skipping result")
                        logging.warning("Match fixture already exists.. skipping result")
                        conn.rollback()
                    else:
                        print(f"Error: {e}")
                        logging.warning(f"Error: {e}")
                        conn.rollback()
                except Exception as e:
                    print(f"Error: {e}")
                    logging.warning(f"Error: {e}")
                    conn.rollback()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')



if __name__ == "__main__":
    logging.basicConfig(filename='webscraper.log', encoding='utf-8', level=logging.DEBUG)
    logging.info("****************************************************")
    logging.info(str(datetime.now()) + ' - footballFixtures.py script started')

    # teamToFind = input("Which football team do you want to get fixtures for: ")
    teamToFind = "Derby County"
    webscrapperBot = ScrapeFixtures()

    matchesList = webscrapperBot.get_match_details(webscrapperBot.get_fixtures_links(teamToFind))
    # webscrapperBot.save_to_csv(matchesList, teamToFind)

    # savedMatches = pd.read_csv("derby-county-fixtures-upcoming.csv")

    matchesdf = pd.DataFrame(matchesList)

    # print(matchesdf)

    webscrapperBot.save_to_PostgresDatabase(matchesdf)
    # webscrapperBot.save_to_PostgresDatabase(savedMatches)

