from bs4 import BeautifulSoup
import requests
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD, DATABASE_USERNAME, DATABASE_HOST
import logging


DATE_FORMAT_STRING = "%d %b %Y"

def findPlayedFixtures():
    try:
        conn = psycopg2.connect(
        database="derbycounty", user=DATABASE_USERNAME, password=DATABASE_PASSWORD, host=DATABASE_HOST, port= '5432'
        )

        cursor = conn.cursor()

        # get all the current fixtures that have been played before today
        currentDate = datetime.now()
        # cursor.execute("SELECT * FROM upcoming_fixtures WHERE kickoff < %s", (currentDate,))
        cursor.execute("DELETE FROM upcoming_fixtures WHERE kickoff < %s RETURNING sky_sports_url, kickoff", (currentDate,))

        FixturesToFindResultFor =  cursor.fetchall()

        print(FixturesToFindResultFor)
    

        conn.commit()


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

    # get the current season from the date
    if fixtureDate > datetime(fixtureDate.year, 7, 31).date():
        season = str(fixtureDate.year)+"-"+str(fixtureDate.year+1)[2:4]
    else:
        season = str(fixtureDate.year-1)+"-"+str(fixtureDate.year)[2:4]
    
    headers = {  
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Cache-Control': 'no-cache'
    }
    r = requests.get(fixtureUrl, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    matchData = soup.find('div', class_='sdc-site-match-header__body')

    matchDetails = matchData.find('div', class_='sdc-site-match-header__detail')

    #scrape the required fields from the webpage
    competition = matchDetails.find('p', class_="sdc-site-match-header__detail-fixture").text.split(".")[1].strip()
    venue = matchDetails.find('span', class_="sdc-site-match-header__detail-venue").text.strip().strip(".")
    home_team = matchData.find('span', class_="sdc-site-match-header__team-name sdc-site-match-header__team-name--home").span.text.strip()
    away_team = matchData.find('span', class_="sdc-site-match-header__team-name sdc-site-match-header__team-name--away").span.text.strip()

    home_score = matchData.find('span', {"data-update" : "score-home"}).text.strip()
    away_score = matchData.find('span', {"data-update" : "score-away"}).text.strip()


    # check if the game was postponed, abandoned or cancelled
    if (home_score == 'P' or home_score == 'A' or home_score == 'C'):
        print("match not played, skipping...")
        return

    penalty_result = matchData.find('p', {"data-update" : "status-note"}).text

    home_score = int(home_score)
    away_score = int(away_score)
    
    # determine if Derby County won or lost
    result = None
    if home_score == away_score:
        result = "D"
    elif home_team == "Derby County" and home_score > away_score:
        result = "W"
    elif away_team == "Derby County" and home_score < away_score:
        result = "W"
    else:
        result = "L"

    # figure out if the game went to penalties are determine if derby won
    if penalty_result != "":
        if penalty_result.startswith("Derby County win"):
            result = "W"
        else:
            result = "L"

    match_data = [home_team, away_team, result, home_score, away_score, fixtureDate, competition.replace("Sky Bet ", ""), season, venue, penalty_result]


    save_to_PostgresDatabase(match_data)

    return match_data
        

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
    logging.info(str(datetime.now()) + ' - newResults.py script started')

    for fixture in findPlayedFixtures():
        print(fixture[0], fixture[1].date())
        scrapeResult(fixture[0], fixture[1].date())



