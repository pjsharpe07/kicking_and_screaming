import duckdb
from itscalledsoccer.client import AmericanSoccerAnalysis


cursor = duckdb.connect("data/kicking_dev.db")

asa_client = AmericanSoccerAnalysis()

team_data = asa_client.get_teams(leagues='mls')

team_conferences = {
    "San Jose Earthquakes" : "West",
    "New England Revolution" : "East",
    "Philadelphia Union" : "East",
    "Real Salt Lake" : "West",
    "New York Red Bulls" : "East",
    "CF Montréal" : "East",
    "D.C. United" : "East",
    "Los Angeles FC" : "West",
    "Austin FC" : "West",
    "Seattle Sounders FC" : "West",
    "Orlando City SC" : "East",
    "LA Galaxy" : "West",
    "Atlanta United FC" : "East",
    "Toronto FC" : "East",
    "Minnesota United FC" : "West",
    "Vancouver Whitecaps FC" : "West",
    "FC Dallas" : "West",
    "Columbus Crew" : "East",
    "Charlotte FC" : "East",
    "FC Cincinnati" : "East",
    "Colorado Rapids" : "West",
    "New York City FC" : "East",
    "Nashville SC" : "East",
    "Portland Timbers FC" : "West",
    "St. Louis City SC" : "West",
    "Chicago Fire FC" : "East",
    "Houston Dynamo FC" : "West",
    "Sporting Kansas City" : "West",
    "Inter Miami CF" : "East"
}

# there is one team that is not really mls here
team_data['conference'] = team_data['team_name'].map(team_conferences)
team_data.dropna(subset='conference', inplace=True)


table_count = cursor.execute('SELECT COUNT(*) FROM main.teams').fetchone()[0]

if table_count > 0:
    print("Must perform some merge argument")
else:
    print("No data found. Proceeding with bulk upload...")
    cursor.sql("INSERT INTO main.teams SELECT * FROM team_data")
    print("Data inserted!")