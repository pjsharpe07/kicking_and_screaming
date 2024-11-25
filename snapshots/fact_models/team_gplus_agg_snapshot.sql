{% snapshot team_gplus_agg_snapshot %}

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
    , total_gplus
    , avg_gplus_per_game
    , team_gplus_rank
    , team_gplus_conf_rank
    , avg_gplus_rank
    , avg_gplus_conf_rank
from {{ ref("team_gplus_agg") }}

{% endsnapshot %}
