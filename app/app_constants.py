##########################################################
# this will have the user pick one of:

# 1. rankings
# 2. team analysis
# 3. player data

# then that will create another menu of tables to choose from
# the list will be below

##########################################################

# this is broken out by the primary 'option'
# which gives a list of the user choices as keys and the corresponding table
user_choice_dict = {
    "rankings" : {
        "Conference Rankings" : "kicking_dev.purty.conference_rankings",
        "League Rankings" : "kicking_dev.purty.league_rankings",
        "Standings" : "kicking_dev.purty.standings",
    },

    "team analysis" : {
        "Home vs Away Analysis" : "kicking_dev.purty.home_away_analysis",
        "Aggregated Stats" : "kicking_dev.purty.aggregate_stats_with_points",
        "Goals Added Aggregated" : "kicking_dev.purty.team_gplus_agg",
        "Goals Added Pivoted" : "kicking_dev.purty.team_gplus_pivot",
        "Team Expected Goals" : "kicking_dev.purty.team_xg_agg",
    },

    "player data" : {
        "Player Expected Goals" : "kicking_dev.purty.player_xg_agg",
        "Player Expected Goals - Most Recent Game" : "purty.player_xg_current_agg"
    }
}

