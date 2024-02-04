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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Cache-Control': 'no-cache'
    }
    team = team.replace(' ', '-').lower()
    url = f"https://www.11v11.com/teams/{team}/tab/matches/"
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    match_content = soup.find('div', {"id": "pageContent"})

    seasons = match_content.find('ul', class_="linkList").find_all('li')


    urls = []

    for season in seasons:
        seasonLink = season.find('a')
        urls.append(seasonLink['href'])

    return urls


def get_all_matches(team, seasonURL):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Cache-Control': 'no-cache'
    }
    team = team.replace(' ', '-').lower()
    r = requests.get(seasonURL, headers=headers)
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
        score = match_data[3].text.strip().split(" ")
        competition = match_data[4].text.strip()

        pensResult = None

        game_score = score[0].split("-")

        if len(score) == 2:
            pensResult = score[1].strip("()")


        data.append([datetime.strptime(date, DATE_FORMAT_STRING).date(), teams[0], teams[1], result, game_score[0], game_score[1], pensResult, competition, season])

    matchesdf = pd.DataFrame(data, columns=["Date", "Home Team", "Away Team", "Result", "Home Score", "Away Score", "Penalties Score", "Competition", "Season"])


    return matchesdf


    matchesdf.to_csv(f"seasonResults/{team.replace(' ', '-').lower()}-match-results-{season}.csv", index=False)

def save_to_PostgresDatabase(df):

    try:
        #establishing the connection
        conn = psycopg2.connect(
        database="derbycounty", user='postgres', password=DATABASE_PASSWORD, host='localhost', port= '5432'
        )

        cursor = conn.cursor()

        cursor.execute("SELECT MAX(id) FROM match_results;")

        currentID = cursor.fetchone()[0]

        if currentID == None:
            currentID = 1
        else:
            currentID += 1

        for i in range(len(df)):

            try:
                print(df.loc[i, "Home Team"], df.loc[i, "Away Team"])

                if df.loc[i, "Penalties Score"] == None:
                    cursor.execute("INSERT INTO match_results(id, home_team, away_team, result, home_score, away_score, kickoff, competition, season) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (currentID, df.loc[i, "Home Team"], df.loc[i, "Away Team"], df.loc[i, "Result"], df.loc[i, "Home Score"], df.loc[i, "Away Score"], df.loc[i, "Date"], df.loc[i, "Competition"], df.loc[i, "Season"]))
                
                else:
                    cursor.execute("INSERT INTO match_results(id, home_team, away_team, result, home_score, away_score, penalties_score, kickoff, competition, season) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (currentID, df.loc[i, "Home Team"], df.loc[i, "Away Team"], df.loc[i, "Result"], df.loc[i, "Home Score"], df.loc[i, "Away Score"], df.loc[i, "Penalties Score"], df.loc[i, "Date"], df.loc[i, "Competition"], df.loc[i, "Season"]))

                conn.commit()
                currentID += 1
            except psycopg2.IntegrityError as e:
                if "duplicate key" in str(e):
                    print("Match result already exists.. skipping result")
                else:
                    print(f"Error: {e}")
                    conn.rollback()  # Roll back the transaction to avoid issues
            except Exception as e:
                print(f"Error: {e}")
                conn.rollback()  # Roll back the transaction for any other unexpected errors
    
    except Exception as e:
        print(f"Error: {e}")

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == "__main__":

    teamToFind = input("Which football team do you want to get match results for: ")
    team = teamToFind.replace(' ', '-').lower()

    findAllSeasons  = input("Do you want the match data for every season or a specific season? Y = all, N = specific season: ").capitalize().strip()

    while (findAllSeasons != "Y" and findAllSeasons != "N"):
        findAllSeasons  = input("Do you want the match data for every season or a specific season? Y = all, N = specific season: ").capitalize()
    
    if (findAllSeasons == "Y"):
        seasonsURLs = get_all_seasons(team)
        for url in seasonsURLs:
            matchesdf = get_all_matches(team, url)
            save_to_PostgresDatabase(matchesdf)
            # print(matchesdf)
    
    else:
        year = input("Which football year do you want the results for: ")
        url = f"https://www.11v11.com/teams/{team}/tab/matches/season/{year}/"
        matchesdf = get_all_matches(team, url)
        save_to_PostgresDatabase(matchesdf)
        # print(matchesdf)




