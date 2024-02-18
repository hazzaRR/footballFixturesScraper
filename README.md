# Football matches, fixtures and table web scraper

Hi welcome to my repository for scraping derby county matches, fixtures and tables built using Python. If you want to view the live site check it out here at [derby-county.harryredman.com](https://derby-county.harryredman.com).

These scripts work by using cron jobs on a raspberry pi 4 to run the scripts using a python virtual environment to keep my PostgreSQL database up to date.

## Setup

Make sure to set up a virtual environment by:

```bash

# create virtual environment
virtualenv webscraper

# install the required dependencies
pip install -r requirements.txt
```

## Cron Job commands to run the scripts

```bash

0 0 1 * * ~/footballFixturesScraper/webscraper/bin/python ~/footballFixturesScraper/footballFixtures.py
0 0 * * * ~/footballFixturesScraper/webscraper/bin/python ~/footballFixturesScraper/newResults.py
0 0 * * * ~/footballFixturesScraper/webscraper/bin/python ~/footballFixturesScraper/leagueTable.py
```

