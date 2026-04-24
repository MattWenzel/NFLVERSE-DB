"""View definitions. One function per view; each returns SQL body string."""

from __future__ import annotations


def v_player_careers_sql() -> str:
    """Per-player career rollup from season_stats.

    Aggregates every player's regular-season totals plus a parallel set
    of postseason totals. One row per player_gsis_id. Players without
    stat rows (e.g. pre-stat-era picks) simply don't appear — consumer
    joins are LEFT JOIN-friendly.

    Column conventions:
      - `career_*`      : regular-season totals
      - `career_post_*` : postseason totals
      - `seasons_played`: distinct REG seasons with a stat line
      - `first_season/last_season`: REG bounds (useful for "active in X" gates)
    """
    return """
        SELECT
            player_gsis_id,
            MAX(player_display_name)                                     AS player_display_name,
            MAX(position)                                                AS position,
            MAX(position_group)                                          AS position_group,
            MIN(season) FILTER (WHERE season_type = 'REG')               AS first_season,
            MAX(season) FILTER (WHERE season_type = 'REG')               AS last_season,
            COUNT(DISTINCT season) FILTER (WHERE season_type = 'REG')    AS seasons_played,

            SUM(games)                 FILTER (WHERE season_type='REG')  AS career_games,
            SUM(completions)           FILTER (WHERE season_type='REG')  AS career_completions,
            SUM(attempts)              FILTER (WHERE season_type='REG')  AS career_attempts,
            SUM(passing_yards)         FILTER (WHERE season_type='REG')  AS career_passing_yards,
            SUM(passing_tds)           FILTER (WHERE season_type='REG')  AS career_passing_tds,
            SUM(passing_interceptions) FILTER (WHERE season_type='REG')  AS career_passing_ints,
            SUM(sacks_suffered)        FILTER (WHERE season_type='REG')  AS career_sacks_suffered,
            SUM(carries)               FILTER (WHERE season_type='REG')  AS career_rush_attempts,
            SUM(rushing_yards)         FILTER (WHERE season_type='REG')  AS career_rushing_yards,
            SUM(rushing_tds)           FILTER (WHERE season_type='REG')  AS career_rushing_tds,
            SUM(receptions)            FILTER (WHERE season_type='REG')  AS career_receptions,
            SUM(targets)               FILTER (WHERE season_type='REG')  AS career_targets,
            SUM(receiving_yards)       FILTER (WHERE season_type='REG')  AS career_receiving_yards,
            SUM(receiving_tds)         FILTER (WHERE season_type='REG')  AS career_receiving_tds,
            SUM(special_teams_tds)     FILTER (WHERE season_type='REG')  AS career_st_tds,
            SUM(def_tackles_solo)      FILTER (WHERE season_type='REG')  AS career_def_tackles_solo,
            SUM(def_sacks)             FILTER (WHERE season_type='REG')  AS career_def_sacks,
            SUM(def_interceptions)     FILTER (WHERE season_type='REG')  AS career_def_ints,
            SUM(def_pass_defended)     FILTER (WHERE season_type='REG')  AS career_def_pass_def,
            SUM(def_fumbles_forced)    FILTER (WHERE season_type='REG')  AS career_def_fumbles_forced,
            SUM(fg_made)               FILTER (WHERE season_type='REG')  AS career_fg_made,
            SUM(fg_att)                FILTER (WHERE season_type='REG')  AS career_fg_att,
            SUM(fantasy_points)        FILTER (WHERE season_type='REG')  AS career_fantasy_points,
            SUM(fantasy_points_ppr)    FILTER (WHERE season_type='REG')  AS career_fantasy_points_ppr,

            SUM(games)             FILTER (WHERE season_type='POST')     AS career_post_games,
            SUM(passing_yards)     FILTER (WHERE season_type='POST')     AS career_post_passing_yards,
            SUM(passing_tds)       FILTER (WHERE season_type='POST')     AS career_post_passing_tds,
            SUM(rushing_yards)     FILTER (WHERE season_type='POST')     AS career_post_rushing_yards,
            SUM(rushing_tds)       FILTER (WHERE season_type='POST')     AS career_post_rushing_tds,
            SUM(receiving_yards)   FILTER (WHERE season_type='POST')     AS career_post_receiving_yards,
            SUM(receiving_tds)     FILTER (WHERE season_type='POST')     AS career_post_receiving_tds,
            SUM(def_sacks)         FILTER (WHERE season_type='POST')     AS career_post_def_sacks,
            SUM(def_interceptions) FILTER (WHERE season_type='POST')     AS career_post_def_ints
        FROM season_stats
        WHERE player_gsis_id IS NOT NULL
        GROUP BY player_gsis_id
    """


def v_draft_pick_careers_sql() -> str:
    """draft_picks + v_player_careers joined on player_gsis_id.

    Consumer use case: given a draft pick (year, round, pick), see what
    the player's career became — in our aggregated form (REG + POST
    from season_stats) rather than PFR's own numbers baked into
    draft_picks (which are REG-only and not consistently updated).

    Left join: keeps pre-GSIS / pre-stat-era picks with career_* as NULL.
    """
    return """
        SELECT
            dp.*,
            pc.first_season                   AS nfl_first_season,
            pc.last_season                    AS nfl_last_season,
            pc.seasons_played                 AS nfl_seasons,
            pc.career_games,
            pc.career_completions,
            pc.career_attempts,
            pc.career_passing_yards,
            pc.career_passing_tds,
            pc.career_passing_ints,
            pc.career_rush_attempts,
            pc.career_rushing_yards,
            pc.career_rushing_tds,
            pc.career_receptions,
            pc.career_targets,
            pc.career_receiving_yards,
            pc.career_receiving_tds,
            pc.career_def_tackles_solo,
            pc.career_def_sacks,
            pc.career_def_ints,
            pc.career_fantasy_points,
            pc.career_fantasy_points_ppr,
            pc.career_post_games,
            pc.career_post_passing_yards,
            pc.career_post_passing_tds,
            pc.career_post_rushing_yards,
            pc.career_post_rushing_tds,
            pc.career_post_receiving_yards,
            pc.career_post_receiving_tds
        FROM draft_picks dp
        LEFT JOIN v_player_careers pc ON pc.player_gsis_id = dp.player_gsis_id
    """


def v_depth_charts_sql() -> str:
    """Composite view across the two depth-chart schemas.

    Pre-2025 rows come from `depth_charts`; 2025+ rows from `depth_charts_2025`
    with derived `season` (NFL calendar: Jan-Feb belong to prior year) and
    `week` (looked up from `games` by gameday ≤ depth chart date). The 2025
    `pos_abb` values (LCB/RCB/WLB/…) are mapped to legacy-comparable generic
    positions (CB/OLB/…) so `WHERE position='CB'` works across both eras.
    """
    return """
        SELECT
            season,
            CAST(week AS INTEGER)                     AS week,
            NULL::VARCHAR                             AS dt,
            club_code                                 AS team,
            player_gsis_id,
            NULL::VARCHAR                             AS player_espn_id,
            position,
            depth_position                            AS pos_abb,
            TRY_CAST(depth_team AS INTEGER)           AS depth_rank,
            formation,
            NULL::VARCHAR                             AS pos_grp,
            'legacy'                                  AS source
        FROM depth_charts
        UNION ALL
        SELECT
            CASE
                WHEN EXTRACT(MONTH FROM CAST(dc.dt AS TIMESTAMP)) <= 2
                    THEN CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER) - 1
                ELSE CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER)
            END                                       AS season,
            (SELECT g.week FROM games g
             WHERE g.season = CASE
                        WHEN EXTRACT(MONTH FROM CAST(dc.dt AS TIMESTAMP)) <= 2
                            THEN CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER) - 1
                        ELSE CAST(strftime(CAST(dc.dt AS TIMESTAMP), '%Y') AS INTEGER)
                    END
               AND CAST(g.gameday AS DATE) <= CAST(dc.dt AS DATE)
             ORDER BY CAST(g.gameday AS DATE) DESC
             LIMIT 1)::INTEGER                        AS week,
            dc.dt,
            dc.team,
            dc.player_gsis_id,
            dc.player_espn_id,
            CASE
                WHEN dc.pos_abb IN ('LT','RT')                   THEN 'T'
                WHEN dc.pos_abb IN ('LG','RG')                   THEN 'G'
                WHEN dc.pos_abb IN ('LDE','RDE')                 THEN 'DE'
                WHEN dc.pos_abb IN ('LDT','RDT')                 THEN 'DT'
                WHEN dc.pos_abb IN ('WLB','SLB','LOLB','ROLB')   THEN 'OLB'
                WHEN dc.pos_abb IN ('RILB','LILB','MLB')         THEN 'ILB'
                WHEN dc.pos_abb IN ('LCB','RCB')                 THEN 'CB'
                WHEN dc.pos_abb IN ('NB','NCB')                  THEN 'NB'
                WHEN dc.pos_abb = 'PK'                           THEN 'K'
                ELSE dc.pos_abb
            END                                       AS position,
            dc.pos_abb,
            dc.pos_rank                               AS depth_rank,
            CASE
                WHEN dc.pos_grp = 'Special Teams' THEN 'Special Teams'
                WHEN dc.pos_grp LIKE 'Base%'      THEN 'Defense'
                ELSE 'Offense'
            END                                       AS formation,
            dc.pos_grp,
            'v2025'                                   AS source
        FROM depth_charts_2025 dc
    """
