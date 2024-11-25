{% snapshot team_xg_agg_snapshot %}

    {{
        config(
          unique_key="team_id || '-' || season_name",
          strategy="check",
          check_cols=['games_played'],
          target_schema="snapshot"
        )
    }}

select
    team_id
  , team_name
  , season_name
  , conference
  , games_played
  , shots_for
  , shots_against
  , goals_for
  , goals_against
  , goal_difference
  , xgoals_for
  , xgoals_against
  , xgoal_difference
from {{ ref("team_xg_agg") }}

{% endsnapshot %}
