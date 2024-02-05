from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD
import os


DATE_FORMAT_STRING = "%d %b %Y"

def findPlayedFixtures():
    try:
    #establishing the connection
        conn = psycopg2.connect(
        database="derbycounty", user='postgres', password=DATABASE_PASSWORD, host='localhost', port= '5432'
        )

        cursor = conn.cursor()

        currentDate = datetime.now()

        cursor.execute("SELECT * FROM upcoming_fixtures WHERE kickoff < %s", (currentDate,))

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

def scrapeResult(fixtureUrl):

        headers = {  
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
            'Cache-Control': 'no-cache'
        }
        r = requests.get(fixtureUrl, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')

        matchData = soup.find_all('div', class_='sdc-site-match-header__body')

        matchDetails = soup.find_all('div', class_='sdc-site-match-header__detail')

        print(matchDetails)


# def save_to_PostgresDatabase(df):

#     try:
#         #establishing the connection
#         conn = psycopg2.connect(
#         database="derbycounty", user='postgres', password=DATABASE_PASSWORD, host='localhost', port= '5432'
#         )

#         cursor = conn.cursor()

#         cursor.execute("SELECT MAX(id) FROM match_results;")

#         currentID = cursor.fetchone()[0]

#         if currentID == None:
#             currentID = 1
#         else:
#             currentID += 1

#         for i in range(len(df)):

#             try:
#                 print(df.loc[i, "Home Team"], df.loc[i, "Away Team"])

#                 if df.loc[i, "Penalties Score"] == None:
#                     cursor.execute("INSERT INTO match_results(id, home_team, away_team, result, home_score, away_score, kickoff, competition, season) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
#                                     (currentID, df.loc[i, "Home Team"], df.loc[i, "Away Team"], df.loc[i, "Result"], df.loc[i, "Home Score"], df.loc[i, "Away Score"], df.loc[i, "Date"], df.loc[i, "Competition"], df.loc[i, "Season"]))
                
#                 else:
#                     cursor.execute("INSERT INTO match_results(id, home_team, away_team, result, home_score, away_score, penalties_score, kickoff, competition, season) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
#                     (currentID, df.loc[i, "Home Team"], df.loc[i, "Away Team"], df.loc[i, "Result"], df.loc[i, "Home Score"], df.loc[i, "Away Score"], df.loc[i, "Penalties Score"], df.loc[i, "Date"], df.loc[i, "Competition"], df.loc[i, "Season"]))

#                 conn.commit()
#                 currentID += 1
#             except psycopg2.IntegrityError as e:
#                 if "duplicate key" in str(e):
#                     print("Match result already exists.. skipping result")
#                 else:
#                     print(f"Error: {e}")
#                     conn.rollback()  # Roll back the transaction to avoid issues
#             except Exception as e:
#                 print(f"Error: {e}")
#                 conn.rollback()  # Roll back the transaction for any other unexpected errors
    
#     except Exception as e:
#         print(f"Error: {e}")

#         cursor.close()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()
#             print('Database connection closed.')


if __name__ == "__main__":

    for fixture in findPlayedFixtures():
        print(scrapeResult(fixture[6]))

    # findPlayedFixtures()




