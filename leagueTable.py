from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pandas as pd
import psycopg2
from credentials import DATABASE_PASSWORD, DATABASE_USERNAME, DATABASE_HOST
import logging


DATE_FORMAT_STRING = "%d %b %Y"

def findLeagueTableLink(teamURL):

    headers = {  
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Cache-Control': 'no-cache'
    }
    r = requests.get(teamURL, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    tableLink = soup.find('a', class_='page-nav__link', string="Tables")


    print(tableLink['href'])

    return tableLink['href']


def scrapeTableData(tableURL):

    LeagueStats = []
    
    headers = {  
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Cache-Control': 'no-cache'
    }
    r = requests.get(tableURL, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    tableData = soup.find('table', class_='standing-table__table')

    tableData = tableData.find('tbody')

    teamStats = tableData.find_all('tr', class_="standing-table__row")



    print(len(teamStats))

    for team in teamStats:
        stats = team.find_all('td', class_="standing-table__cell")

        teamStats = {
                'League Position': stats[0].text,
                'Team': stats[1].a.text,
                'Played': stats[2].text,
                'Wins': stats[3].text,
                'Draws': stats[4].text,
                'Losses': stats[5].text,
                'Goals For': stats[6].text,
                'Goals Against': stats[7].text,
                'Goals Difference': stats[8].text,
                'Points': stats[9].text,
        }  

        LeagueStats.append(teamStats)

    return LeagueStats

        

def save_to_PostgresDatabase(match_data):

    try:
        conn = psycopg2.connect(
        database="derbycounty", user=DATABASE_USERNAME, password=DATABASE_PASSWORD, host=DATABASE_HOST, port= '5432'
        )

        cursor = conn.cursor()

        cursor.execute("SELECT MAX(id) FROM match_results;")

        currentID = cursor.fetchone()[0]

        if currentID == None:
            currentID = 1
        else:
            currentID += 1

            try:
                print(match_data[0], match_data[1])

                if match_data[8] == "":
                    cursor.execute("INSERT INTO match_results(id, home_team, away_team, result, home_score, away_score, kickoff, competition, season, stadium) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (currentID, match_data[0], match_data[1], match_data[2], match_data[3], match_data[4], match_data[5], match_data[6], match_data[7], match_data[8]))
                
                else:
                    cursor.execute("INSERT INTO match_results(id, home_team, away_team, result, home_score, away_score, kickoff, competition, season, stadium, penalties_score) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (currentID, match_data[0], match_data[1], match_data[2], match_data[3], match_data[4], match_data[5], match_data[6], match_data[7], match_data[8], match_data[9]))

                conn.commit()
                currentID += 1
            except psycopg2.IntegrityError as e:
                if "duplicate key" in str(e):
                    print("Match result already exists.. skipping result")
                    logging.warning('Match result already exists.. skipping result')
                else:
                    print(f"Error: {e}")
                    logging.warning(f"Error: {e}")
                    conn.rollback()  # Roll back the transaction to avoid issues
            except Exception as e:
                print(f"Error: {e}")
                logging.warning(f"Error: {e}")
                conn.rollback()  # Roll back the transaction for any other unexpected errors
    
    except Exception as e:
        print(f"Error: {e}")
        logging.warning(f"Error: {e}")
        cursor.close()
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == "__main__":
    logging.basicConfig(filename='webscraper.log', encoding='utf-8', level=logging.DEBUG)
    logging.info("****************************************************")
    logging.info(str(datetime.now()) + ' - leagueTable.py script started')


    tableURL = findLeagueTableLink("https://www.skysports.com/derby-county")

    leagueStats = scrapeTableData(f"https://www.skysports.com{tableURL}")

    for teamStat in leagueStats:
        print(teamStat)



