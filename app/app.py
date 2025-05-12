import streamlit as st
import duckdb
import os
import altair as alt
from app_constants import user_choice_dict
import sys
from pathlib import Path

# Add the project root to Python path to enable absolute imports
sys.path.append(str(Path(__file__).parent.parent.resolve()))
from raw_data_etl.config import path_to_database

con = duckdb.connect(database=path_to_database, read_only=True)

st.set_page_config(layout="wide")
st.title("Exploring some ASA Data For MLS")

with st.expander("About the data"):
    st.write(
        """This data is pulled from the ASA API and is just for me!
You have some options to choose from:
1. Rankings - league and conference rankings with statistics and rankings
2. Team Analysis - A lot of numbers at a team level
3. Player Data - A lot of numbers at a player level
"""
    )

###########################################
######### Set up user choices #############
###########################################

st.subheader("What data do you want to see?")


# we organized all of these tables in the constants file
# so we allow the user to filter to what they want to see
user_choice_col_1, user_choice_col_2, user_choice_col_3 = st.columns([0.4, 0.4, 0.2])

initial_user_option = list(user_choice_dict.keys())

with user_choice_col_1:
    user_parent_choice = st.selectbox("What Category?", initial_user_option)

user_parent_dictionary = user_choice_dict.get(user_parent_choice)
user_table_choices = list(user_parent_dictionary.keys())

with user_choice_col_2:
    user_table_choice = st.selectbox("Which table?", user_table_choices)

# establish the years
with user_choice_col_3:
    distinct_year_query = """
    SELECT DISTINCT season_name
    FROM purty.league_rankings
    """

    years = [x[0] for x in con.execute(distinct_year_query).fetchall()]

    year = st.selectbox("Which season?", years)

current_table = user_parent_dictionary.get(user_table_choice)


##########################################
############# filters ####################
##########################################


if user_parent_choice != "player data":

    st.write("Do you want to filter teams? If so, which ones?")

    col_a1, col_a2, col_a3 = st.columns(3)

    distinct_teams_query = """
    SELECT DISTINCT team_name
    FROM raw.team_conferences
    ORDER BY team_name
    """

    teams = [x[0] for x in con.execute(distinct_teams_query).fetchall()]

    with col_a1:
        use_filters = st.selectbox("Filter To Specific Teams?", [True, False])

    with col_a2:
        team_one = st.selectbox("Team One", teams)

    with col_a3:
        second_team_list = [x for x in teams if x != team_one]
        team_two = st.selectbox("Team Two", second_team_list)

    if use_filters:
        where_clause = f"""
        WHERE team_name IN ('{team_one}', '{team_two}')
        AND season_name = {year}
        """

    # we want to filter by year no matter what
    else:
        where_clause = f"WHERE season_name = {year}"
else:
    where_clause = f"WHERE season_name = {year}"

# ##########################################
# ########### data preview #################
# ##########################################

st.subheader("Table Data")


main_table_query = f"""
SELECT *
FROM {current_table}
{where_clause}
"""

main_table_df = con.execute(main_table_query).df()

st.write(main_table_df)


# ##########################################
# ############# charts #####################
# ##########################################

# metadata about the different charts
# based on user input
# then returns the table_name followed by the columns
metric_data = {
    "Team XG": {
        "table_name": "team_xg_agg_snapshot",
        "metric_columns": [
            "goals_for",
            "goals_against",
            "goal_difference",
            "xgoals_for",
            "xgoals_against",
            "xgoal_difference",
        ],
        "chart_title": "Team XG Over Games Played",
    },
    "Team Goals Added": {
        "table_name": "team_gplus_agg_snapshot",
        "metric_columns": ["total_gplus", "avg_gplus_per_game"],
        "chart_title": "Team G+ Over Games Played",
    },
}

st.subheader("Charts")


# fetch user metadata about the chart they want to view
st.write("Filter to year, metric, and teams if you want")
col_a1, col_a2, col_a3 = st.columns(3)

with col_a1:
    metric_year = st.selectbox("Which year?", years)

with col_a2:
    metric_table = st.selectbox("Which chart?", metric_data.keys())
with col_a3:
    metric = st.selectbox("Which metric?", metric_data[metric_table]["metric_columns"])


#### get the filter for the teams
filter_one, filter_two = st.columns(2)

with filter_one:
    first_team = st.selectbox("First Team", teams)

with filter_two:
    second_team_list = [x for x in teams if x != first_team]
    second_team = st.selectbox("Second Team", second_team_list)

# now start fetching the metadata about each
chart_data = metric_data[metric_table]
table_name = chart_data["table_name"]
chart_title = chart_data["chart_title"]

# perform query, fetch results to df, then pivot!
chart_query = f"""
SELECT team_name, games_played, {metric}
FROM snapshot.{table_name}
WHERE season_name = {metric_year}
AND team_name IN ('{first_team}', '{second_team}')
"""

chart_df = con.execute(chart_query).df()
pivot_df = chart_df.pivot(index="games_played", columns="team_name", values=metric)

st.title(chart_title)
st.line_chart(pivot_df)
