import streamlit as st
import duckdb
import os
import altair as alt
from app_constants import user_choice_dict

path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")
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

user_choice_col_1, user_choice_col_2 = st.columns(2)

initial_user_option = list(user_choice_dict.keys())

with user_choice_col_1:
    user_parent_choice = st.selectbox("What Category?", initial_user_option)

user_parent_dictionary = user_choice_dict.get(user_parent_choice)
user_table_choices = list(user_parent_dictionary.keys())

with user_choice_col_2:
    user_table_choice = st.selectbox("Which table?", user_table_choices)

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
        use_filters = st.selectbox("Filter Results?", [True, False])

    with col_a2:
        team_one = st.selectbox("Team One", teams)

    with col_a3:
        second_team_list = [x for x in teams if x != team_one]
        team_two = st.selectbox("Team Two", second_team_list)

where_clause = f"WHERE team_name IN ('{team_one}', '{team_two}')" if use_filters else ""

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

st.subheader("Charts")

distinct_teams_query = """
DESCRIBE SELECT * EXCLUDE(team_id, team_name, conference, total_points, points_per_game, season_name)
FROM kicking_dev.purty.aggregate_stats_with_points
"""

compare_values = [x[0] for x in con.execute(distinct_teams_query).fetchall()]

x_value = st.selectbox("Which value do you want to compare?", compare_values)

chart_table_query = f"""
SELECT {x_value}, points_per_game, conference, team_name
FROM purty.aggregate_stats_with_points
"""

chart_table_df = con.execute(chart_table_query).df()

query = f"""
SELECT MIN({x_value}) * 0.9, MAX({x_value}) * 1.1
FROM purty.aggregate_stats_with_points
"""

x_lower_bound, x_upper_boud = con.execute(query).fetchone()


chart = (
    alt.Chart(chart_table_df)
    .mark_circle()
    .encode(
        x=alt.X(x_value, scale=alt.Scale(domain=(x_lower_bound, x_upper_boud))),
        y="points_per_game",
        color="conference",
    )
    .interactive()
)

st.altair_chart(chart, theme="streamlit", use_container_width=True)
