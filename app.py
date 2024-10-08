import streamlit as st
import duckdb
import os


path_to_database = os.path.join(os.getcwd(), "data", "kicking_dev.db")
con = duckdb.connect(database=path_to_database, read_only=True)
st.set_page_config(layout="wide")
st.title("Exploring some ASA Data For MLS")

with st.expander('About the data'):
    st.write("This data is pulled from the ASA API and is just for me!")
    # st.image('https://1000logos.net/wp-content/uploads/2021/11/Expedia-Logo.png', width=250)

st.subheader('Filters')

col_a1, col_a2 = st.columns(2)

distinct_teams_query = """
SELECT DISTINCT team_name
FROM raw.team_conferences
ORDER BY team_name
"""

teams = [
    x[0] for x in
    con.execute(distinct_teams_query).fetchall()
]


with col_a1: 
    team_one = st.selectbox('Team One', teams)

with col_a2:
    team_two = st.selectbox("Team Two", teams)


main_table_query = f"""
SELECT *
FROM purty.aggregate_stats_with_points
WHERE team_name IN ('{team_one}', '{team_two}')
ORDER BY team_name
"""

main_table_df = con.execute(main_table_query).df()

st.write('First 10 rows: ', main_table_df)
