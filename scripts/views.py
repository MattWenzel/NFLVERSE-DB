"""View definitions. One function per view; each returns SQL body string."""

from __future__ import annotations


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
