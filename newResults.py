from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD
import os
import footballResults


DATE_FORMAT_STRING = "%d %b %Y"

def findPlayedFixtures():
    try:
    #establishing the connection
        conn = psycopg2.connect(
        database="derbycounty", user='postgres', password=DATABASE_PASSWORD, host='localhost', port= '5432'
        )

        cursor = conn.cursor()

        currentDate = datetime.now()

        cursor.execute("SELECT * FROM upcoming_fixtures WHERE kickoff > %s", (currentDate,))

        FixturesToFindResultFor =  cursor.fetchall()

    except psycopg2.IntegrityError as e:
        if "duplicate key" in str(e):
            print("Match result already exists.. skipping result")
        else:
            print(f"Error: {e}")
            conn.rollback()  
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return FixturesToFindResultFor

def scrapeResult(fixtureUrl, fixtureDate):
    
    season = None

    if fixtureDate > datetime(fixtureDate.year, 7, 31).date():
        season = str(fixtureDate.year)+"-"+str(fixtureDate.year+1)[2:4]
    else:
        season = str(fixtureDate.year-1)+"-"+str(fixtureDate.year)[2:4]
    
    print(season)

    # headers = {  
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    #     'Cache-Control': 'no-cache'
    # }
    # r = requests.get(fixtureUrl, headers=headers)
    # soup = BeautifulSoup(r.text, 'html.parser')

    # matchData = soup.find('div', class_='sdc-site-match-header__body')

    # matchDetails = matchData.find('div', class_='sdc-site-match-header__detail')

    # competition = matchDetails.find('p', class_="sdc-site-match-header__detail-fixture").text.split(".")[1].strip()
    # venue = matchDetails.find('span', class_="sdc-site-match-header__detail-venue").text.strip().strip(".")
    # home_team = matchData.find('span', class_="sdc-site-match-header__team-name sdc-site-match-header__team-name--home").span.text.strip()
    # away_team = matchData.find('span', class_="sdc-site-match-header__team-name sdc-site-match-header__team-name--away").span.text.strip()
    # home_score = int(matchData.find('span', {"data-update" : "score-home"}).text)
    # away_score = int(matchData.find('span', {"data-update" : "score-away"}).text)
    


    # # determine if Derby County won or lost
    # result = None
    # if home_score == away_score:
    #     result = "D"
    # elif home_team == "Derby County" and home_score > away_score:
    #     result = "W"
    # elif away_team == "Derby County" and home_score < away_score:
    #     result = "W"
    # else:
    #     result = "L"


    # print(competition)
    # print(venue)
    # print(home_team)
    # print(away_team)
    # print(home_score)
    # print(away_score)
    # print(result)


if __name__ == "__main__":

    # for fixture in findPlayedFixtures():
    #     print(scrapeResult(fixture[6],fixture[4].date()))
        # print(fixture[4].date()) ##fetch just the date to save

    # findPlayedFixtures()
    print("*****************************************")
    scrapeResult("https://www.skysports.com/football/derby-county-vs-portsmouth/483683", datetime(2023, 9, 12).date())
    print("*****************************************")
    scrapeResult("https://www.skysports.com/football/derby-county-vs-lincoln-city/495048", datetime.now().date())
    print("*****************************************")
    scrapeResult("https://www.skysports.com/football/derby-county-vs-peterborough-united/483904", datetime.now().date())
    print("*****************************************")
    scrapeResult("https://www.skysports.com/football/derby-county-vs-lincoln-city/495048", datetime.now().date())
    print("*****************************************")
    scrapeResult("https://www.skysports.com/football/reading-vs-derby-county/483811", datetime.now().date())
    print("*****************************************")
    scrapeResult("https://www.skysports.com/football/leyton-orient-vs-derby-county/483843", datetime.now().date())



