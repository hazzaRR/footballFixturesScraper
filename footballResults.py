from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import psycopg2
from credentials import DATABASE_PASSWORD
import os


if __name__ == "__main__":

    teamToFind = input("Which football team do you want to get fixtures for: ")

