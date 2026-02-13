# nflverse Database Documentation

Comprehensive NFL player statistics database built from [nflverse](https://github.com/nflverse) data. Uses nflverse-native column names throughout (no custom renames).

## Quick Stats

| Database | Size | Tables | Total Rows | Years |
|----------|------|--------|------------|-------|
| `nflverse_v2.db` | 327 MB | 13 | ~2.25M | 1999-2025 |
| `pbp_v2.db` | 2,082 MB | 1 | 1.28M | 1999-2025 |

Legacy DBs (`nflverse_custom.db`, `pbp.db`) use old custom column renames — do not mix with v2.

### Table Row Counts

| Table | Rows | Columns | Description |
|-------|------|---------|-------------|
| **players** | 24,356 | 39 | Master player registry (all positions) |
| **player_ids** | 7,705 | 35 | Cross-platform ID mapping (ESPN, Yahoo, PFR, etc.) |
| **games** | 7,276 | 46 | Schedule, scores, weather, betting, QB IDs |
| **game_stats** | 475,626 | 115 | Weekly player stats (all position groups) |
| **season_stats** | 49,489 | 113 | Aggregated season totals |
| **draft_picks** | 12,670 | 36 | Historical NFL draft data + career stats |
| **combine** | 8,649 | 18 | Scouting Combine results |
| **snap_counts** | 276,948 | 16 | Weekly snap participation (2015-2025) |
| **ngs_stats** | 26,656 | 52 | Next Gen Stats (2016-2025) |
| **depth_charts** | 869,185 | 15 | Weekly depth charts (2001-2024) |
| **depth_charts_2025** | 476,501 | 12 | Daily depth charts (2025+, different schema) |
| **pfr_advanced** | 7,798 | 64 | PFR advanced stats (2018-2025) |
| **qbr** | 9,570 | 30 | ESPN Total QBR (2006-2023) |
| **play_by_play** | 1,279,628 | 372 | Every NFL play (pbp_v2.db) |

---

## Schema Overview

```
                          nflverse_v2.db
┌──────────────────────────────────────────────────────────────────────────┐
│                              PLAYERS                                    │
│   gsis_id (PK) | display_name | position | latest_team | headshot | ...│
│   39 columns including ngs_position, otc_id, smart_id                  │
└──────────────────────────────────────────────────────────────────────────┘
         │
         │ FK: player_id = players.gsis_id
         ▼
┌─────────────────────────┐        ┌─────────────────────────┐
│       GAME_STATS        │        │      SEASON_STATS       │
│  player_id + season +   │        │  player_id + season     │
│  week (475K rows)       │        │  (49K rows)             │
│  115 columns: offense,  │        │  113 columns: aggregated│
│  defense, kicking, ST   │        │  totals, recent_team    │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│       PLAYER_IDS        │        │          GAMES          │
│  35 columns: cross-ref  │        │  46 columns: schedule,  │
│  to ESPN, Yahoo, PFR... │        │  scores, odds, QB IDs   │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│      DRAFT_PICKS        │        │        COMBINE          │
│  36 columns: draft data │        │  18 columns: measurables│
│  + career stats, HOF    │        │  pos, ht, wt, forty...  │
│  1980-2025              │        │  2000-2025              │
└─────────────────────────┘        └─────────────────────────┘

                    SUPPLEMENTARY TABLES (Advanced Analytics)

┌─────────────────────────┐        ┌─────────────────────────┐
│      SNAP_COUNTS        │        │        NGS_STATS        │
│  Offense/Def/ST snaps   │        │  Next Gen Stats         │
│  Uses PFR ID            │        │  Uses GSIS ID           │
│  2015-2025              │        │  2016-2025              │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│      DEPTH_CHARTS       │        │   DEPTH_CHARTS_2025     │
│  Weekly positions       │        │  Daily positions        │
│  Uses GSIS ID           │        │  Uses GSIS ID + ESPN ID │
│  2001-2024              │        │  2025+ (different schema)│
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│      PFR_ADVANCED       │        │          QBR            │
│  Pressure, drops, etc   │        │  ESPN Total QBR         │
│  Uses PFR ID            │        │  Uses ESPN ID           │
│  2018-2025              │        │  2006-2023              │
└─────────────────────────┘        └─────────────────────────┘

                              pbp_v2.db (separate)
┌──────────────────────────────────────────────────────────────────────────┐
│                           PLAY_BY_PLAY                                  │
│     game_id + play_id | 372 columns | EPA/WPA/CPOE | Player IDs (GSIS) │
│     1.28M plays (1999-2025)                                            │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## ID System & Relationships

### Primary Key: GSIS ID

Most tables use the NFL's **Game Statistics & Information System ID (GSIS ID)** as the primary identifier:
- Format: `00-0033873` (Patrick Mahomes)
- Column name varies: `gsis_id` in `players`, `player_id` in `game_stats`/`season_stats`
- Used in: `players`, `game_stats`, `season_stats`, `ngs_stats`, `depth_charts`, `depth_charts_2025`, `pbp_v2.db`

### Key Join: game_stats ↔ players

The main stats tables use `player_id` while players uses `gsis_id` — same GSIS format, different column names:

```sql
-- game_stats.player_id = players.gsis_id (both are GSIS IDs)
SELECT p.display_name, g.passing_yards
FROM game_stats g
JOIN players p ON g.player_id = p.gsis_id;
```

### Supplementary Table ID Mapping

Different data sources use different ID systems. The `player_ids` table provides cross-references:

| Table | ID Column | Format | Join Path |
|-------|-----------|--------|-----------|
| `game_stats` | `player_id` | `00-0033873` (GSIS) | Direct to `players.gsis_id` |
| `season_stats` | `player_id` | `00-0033873` (GSIS) | Direct to `players.gsis_id` |
| `ngs_stats` | `player_gsis_id` | `00-0035228` | Direct to `players.gsis_id` |
| `depth_charts` | `gsis_id` | `00-0035228` | Direct to `players.gsis_id` |
| `depth_charts_2025` | `gsis_id` | `00-0035228` | Direct to `players.gsis_id` |
| `snap_counts` | `pfr_player_id` | `MahoPa00` | Via `player_ids.pfr_id` → `gsis_id` |
| `pfr_advanced` | `pfr_id` | `MahoPa00` | Via `player_ids.pfr_id` → `gsis_id` |
| `qbr` | `player_id` | `3139477` (ESPN) | Via `player_ids.espn_id` → `gsis_id` |
| `pbp_v2.db` | `*_player_id` | `00-0035228` | Direct to `players.gsis_id` |

### Join Examples

```sql
-- Join snap_counts to players via PFR ID
SELECT p.display_name, sc.offense_snaps, sc.offense_pct
FROM snap_counts sc
JOIN player_ids pi ON sc.pfr_player_id = pi.pfr_id
JOIN players p ON pi.gsis_id = p.gsis_id
WHERE sc.season = 2024 AND sc.week = 1;

-- Join QBR to players via ESPN ID
SELECT p.display_name, q.qbr_total, q.pts_added
FROM qbr q
JOIN player_ids pi ON q.player_id = pi.espn_id
JOIN players p ON pi.gsis_id = p.gsis_id
WHERE q.season = 2023;
```

---

## Core Tables

### Table: `players`

Master registry of all NFL players with biographical and career information.

**Rows:** 24,356 | **Years:** 1999-2025 | **Columns:** 39

| Column | Type | Description |
|--------|------|-------------|
| `gsis_id` | TEXT | **Primary Key** - NFL GSIS ID (`00-0033873`) |
| `display_name` | TEXT | Full display name (e.g., "Patrick Mahomes") |
| `common_first_name` | TEXT | Common first name |
| `first_name` | TEXT | First name |
| `last_name` | TEXT | Last name |
| `short_name` | TEXT | Short name (e.g., "P.Mahomes") |
| `football_name` | TEXT | Football name (e.g., "Pat") |
| `suffix` | TEXT | Name suffix (Jr., III, etc.) |
| `esb_id` | TEXT | Elias Sports Bureau ID |
| `nfl_id` | TEXT | NFL.com ID |
| `pfr_id` | TEXT | Pro Football Reference ID |
| `pff_id` | TEXT | Pro Football Focus ID |
| `otc_id` | TEXT | Over The Cap ID |
| `espn_id` | TEXT | ESPN player ID |
| `smart_id` | TEXT | Smart ID |
| `birth_date` | TEXT | Birth date (YYYY-MM-DD) |
| `position_group` | TEXT | Position group (QB, RB, WR, TE, OL, DL, LB, DB, SPEC) |
| `position` | TEXT | Position (QB, RB, WR, TE, K, P, OL, DL, LB, DB, etc.) |
| `ngs_position_group` | TEXT | NGS position group classification |
| `ngs_position` | TEXT | NGS position classification |
| `height` | REAL | Height in inches |
| `weight` | REAL | Weight in pounds |
| `headshot` | TEXT | URL to NFL CDN headshot image |
| `college_name` | TEXT | College attended |
| `college_conference` | TEXT | College conference |
| `jersey_number` | TEXT | Jersey number |
| `rookie_season` | INTEGER | First NFL season |
| `last_season` | INTEGER | Most recent NFL season |
| `latest_team` | TEXT | Current/most recent team abbreviation |
| `status` | TEXT | Current status (ACT, RES, CUT, etc.) |
| `ngs_status` | TEXT | NGS status |
| `ngs_status_short_description` | TEXT | NGS status description |
| `years_of_experience` | INTEGER | Years in NFL |
| `pff_position` | TEXT | PFF position classification |
| `pff_status` | TEXT | PFF status |
| `draft_year` | REAL | Year drafted |
| `draft_round` | REAL | Draft round |
| `draft_pick` | REAL | Overall draft pick |
| `draft_team` | TEXT | Team that drafted player |

**Example Row (Patrick Mahomes):**
```
gsis_id: 00-0033873
display_name: Patrick Mahomes
position: QB
latest_team: KC
height: 74.0 (6'2")
weight: 225.0
college_name: Texas Tech
headshot: https://static.www.nfl.com/image/upload/...
rookie_season: 2017
pfr_id: MahoPa00
espn_id: 3139477
draft_year: 2017, draft_round: 1, draft_pick: 10
```

**Notable v2 column name changes from legacy:**
- `latest_team` (was `current_team`)
- `headshot` (was `headshot_url`)
- `college_name` (was `college`)

---

### Table: `player_ids`

Cross-reference table mapping GSIS IDs to 20+ other platforms.

**Rows:** 7,705 | **Columns:** 35

| Column | Type | Description |
|--------|------|-------------|
| `gsis_id` | TEXT | **Primary Key** - NFL GSIS ID |
| `name` | TEXT | Player name |
| `merge_name` | TEXT | Normalized name for matching |
| `position` | TEXT | Position |
| `team` | TEXT | Team |
| `mfl_id` | INTEGER | MyFantasyLeague ID |
| `sportradar_id` | TEXT | Sportradar UUID |
| `fantasypros_id` | REAL | FantasyPros ID |
| `pff_id` | REAL | Pro Football Focus ID |
| `sleeper_id` | REAL | Sleeper app ID |
| `nfl_id` | REAL | NFL.com ID |
| `espn_id` | REAL | ESPN ID |
| `yahoo_id` | TEXT | Yahoo Fantasy ID |
| `fleaflicker_id` | TEXT | Fleaflicker ID |
| `cbs_id` | REAL | CBS Sports ID |
| `pfr_id` | TEXT | Pro Football Reference ID (`MahoPa00`) |
| `cfbref_id` | TEXT | College Football Reference ID |
| `rotowire_id` | REAL | Rotowire ID |
| `rotoworld_id` | TEXT | Rotoworld ID |
| `ktc_id` | REAL | KeepTradeCut ID |
| `stats_id` | REAL | Stats Inc ID |
| `stats_global_id` | REAL | Stats Global ID |
| `fantasy_data_id` | REAL | FantasyData API ID |
| `swish_id` | TEXT | Swish Analytics ID |
| `birthdate` | TEXT | Birth date |
| `age` | REAL | Age |
| `draft_year` | REAL | Draft year |
| `draft_round` | REAL | Draft round |
| `draft_pick` | REAL | Draft pick |
| `draft_ovr` | REAL | Overall draft pick |
| `twitter_username` | TEXT | Twitter/X username |
| `height` | REAL | Height |
| `weight` | REAL | Weight |
| `college` | TEXT | College |
| `db_season` | INTEGER | Database season |

**Example Row (Patrick Mahomes):**
```
gsis_id: 00-0033873
pfr_id: MahoPa00
espn_id: 3139477
yahoo_id: 30123
sleeper_id: 4046
sportradar_id: 11cad59d-90dd-449c-a839-dddaba4fe16c
```

---

### Table: `games`

NFL game schedule with scores, venue, weather, betting, and starting QB information.

**Rows:** 7,276 | **Years:** 1999-2025 | **Columns:** 46

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | TEXT | **Primary Key** - Format: `2024_01_KC_BAL` |
| `season` | INTEGER | Season year |
| `game_type` | TEXT | REG, WC, DIV, CON, SB |
| `week` | INTEGER | Week number (1-18 regular, 19-22 playoffs) |
| `gameday` | TEXT | Game date (YYYY-MM-DD) |
| `weekday` | TEXT | Day of week |
| `gametime` | TEXT | Kickoff time |
| `away_team` | TEXT | Away team abbreviation |
| `away_score` | INTEGER | Away team final score |
| `home_team` | TEXT | Home team abbreviation |
| `home_score` | INTEGER | Home team final score |
| `location` | TEXT | Game location |
| `result` | INTEGER | Result from home team perspective |
| `total` | INTEGER | Total points scored |
| `overtime` | INTEGER | 1 if overtime, 0 otherwise |
| `old_game_id` | TEXT | Legacy game ID format |
| `gsis` | INTEGER | GSIS game ID |
| `nfl_detail_id` | TEXT | NFL detail ID |
| `pfr` | TEXT | PFR game ID |
| `pff` | REAL | PFF game ID |
| `espn` | TEXT | ESPN game ID |
| `ftn` | REAL | FTN game ID |
| `away_rest` | INTEGER | Days rest for away team |
| `home_rest` | INTEGER | Days rest for home team |
| `away_moneyline` | REAL | Away team moneyline |
| `home_moneyline` | REAL | Home team moneyline |
| `spread_line` | REAL | Point spread (home team) |
| `away_spread_odds` | REAL | Away spread odds |
| `home_spread_odds` | REAL | Home spread odds |
| `total_line` | REAL | Over/under line |
| `under_odds` | REAL | Under odds |
| `over_odds` | REAL | Over odds |
| `div_game` | INTEGER | 1 if divisional game |
| `roof` | TEXT | Roof type (outdoors, dome, open, closed) |
| `surface` | TEXT | Playing surface |
| `temp` | REAL | Temperature (F) |
| `wind` | REAL | Wind speed (mph) |
| `away_qb_id` | TEXT | Away starting QB GSIS ID |
| `home_qb_id` | TEXT | Home starting QB GSIS ID |
| `away_qb_name` | TEXT | Away starting QB name |
| `home_qb_name` | TEXT | Home starting QB name |
| `away_coach` | TEXT | Away team head coach |
| `home_coach` | TEXT | Home team head coach |
| `referee` | TEXT | Head referee |
| `stadium_id` | TEXT | Stadium ID |
| `stadium` | TEXT | Stadium name |

---

### Table: `game_stats`

Weekly player statistics. **This is the main stats table.**

Contains ALL stat columns for ALL position groups — offensive, defensive, kicking, special teams, penalties, returns, and fumble recovery. RBs can have passing stats (trick plays), defensive players have tackle/sack/INT columns, kickers have full FG/PAT breakdowns, etc.

**Rows:** 475,626 | **Years:** 1999-2025 | **Columns:** 115

#### Identification (11 columns)

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | TEXT | **FK** → `players.gsis_id` (GSIS format) |
| `player_name` | TEXT | Player name |
| `player_display_name` | TEXT | Full display name |
| `position` | TEXT | Position |
| `position_group` | TEXT | Position group |
| `headshot_url` | TEXT | Headshot image URL |
| `season` | INTEGER | Season year |
| `week` | INTEGER | Week number |
| `season_type` | TEXT | REG or POST |
| `team` | TEXT | Player's team for this game |
| `opponent_team` | TEXT | Opponent team |

#### Passing (16 columns)

| Column | Type | Description |
|--------|------|-------------|
| `completions` | INTEGER | Completions |
| `attempts` | INTEGER | Pass attempts |
| `passing_yards` | INTEGER | Passing yards |
| `passing_tds` | INTEGER | Passing touchdowns |
| `passing_interceptions` | INTEGER | Interceptions thrown |
| `sacks_suffered` | INTEGER | Times sacked |
| `sack_yards_lost` | INTEGER | Yards lost to sacks |
| `sack_fumbles` | INTEGER | Fumbles on sacks |
| `sack_fumbles_lost` | INTEGER | Fumbles lost on sacks |
| `passing_air_yards` | INTEGER | Air yards (depth of target) |
| `passing_yards_after_catch` | INTEGER | YAC on completions |
| `passing_first_downs` | INTEGER | First downs via passing |
| `passing_epa` | REAL | Expected Points Added (passing) |
| `passing_cpoe` | REAL | Completion % Over Expected |
| `passing_2pt_conversions` | INTEGER | 2-point conversions (passing) |
| `pacr` | REAL | Passer Air Conversion Ratio |

#### Rushing (8 columns)

| Column | Type | Description |
|--------|------|-------------|
| `carries` | INTEGER | Rush attempts |
| `rushing_yards` | INTEGER | Rushing yards |
| `rushing_tds` | INTEGER | Rushing touchdowns |
| `rushing_fumbles` | INTEGER | Fumbles on rushes |
| `rushing_fumbles_lost` | INTEGER | Fumbles lost on rushes |
| `rushing_first_downs` | INTEGER | First downs via rushing |
| `rushing_epa` | REAL | Expected Points Added (rushing) |
| `rushing_2pt_conversions` | INTEGER | 2-point conversions (rushing) |

#### Receiving (15 columns)

| Column | Type | Description |
|--------|------|-------------|
| `receptions` | INTEGER | Receptions |
| `targets` | INTEGER | Times targeted |
| `receiving_yards` | INTEGER | Receiving yards |
| `receiving_tds` | INTEGER | Receiving touchdowns |
| `receiving_fumbles` | INTEGER | Fumbles after catch |
| `receiving_fumbles_lost` | INTEGER | Fumbles lost after catch |
| `receiving_air_yards` | INTEGER | Air yards on targets |
| `receiving_yards_after_catch` | INTEGER | Yards after catch |
| `receiving_first_downs` | INTEGER | First downs via receiving |
| `receiving_epa` | REAL | Expected Points Added (receiving) |
| `receiving_2pt_conversions` | INTEGER | 2-point conversions (receiving) |
| `racr` | REAL | Receiver Air Conversion Ratio |
| `target_share` | REAL | % of team targets |
| `air_yards_share` | REAL | % of team air yards |
| `wopr` | REAL | Weighted Opportunity Rating |

#### Defensive (15 columns)

| Column | Type | Description |
|--------|------|-------------|
| `def_tackles_solo` | INTEGER | Solo tackles |
| `def_tackles_with_assist` | INTEGER | Tackles with assist |
| `def_tackle_assists` | INTEGER | Tackle assists |
| `def_tackles_for_loss` | INTEGER | Tackles for loss |
| `def_tackles_for_loss_yards` | INTEGER | TFL yards |
| `def_fumbles_forced` | INTEGER | Forced fumbles |
| `def_sacks` | REAL | Sacks |
| `def_sack_yards` | REAL | Sack yards |
| `def_qb_hits` | INTEGER | QB hits |
| `def_interceptions` | INTEGER | Interceptions |
| `def_interception_yards` | INTEGER | Interception return yards |
| `def_pass_defended` | INTEGER | Passes defended |
| `def_tds` | INTEGER | Defensive touchdowns |
| `def_fumbles` | INTEGER | Defensive fumbles |
| `def_safeties` | INTEGER | Safeties |

#### Special Teams & Returns (5 columns)

| Column | Type | Description |
|--------|------|-------------|
| `special_teams_tds` | INTEGER | Return touchdowns |
| `punt_returns` | INTEGER | Punt returns |
| `punt_return_yards` | INTEGER | Punt return yards |
| `kickoff_returns` | INTEGER | Kickoff returns |
| `kickoff_return_yards` | INTEGER | Kickoff return yards |

#### Fumble Recovery & Misc (6 columns)

| Column | Type | Description |
|--------|------|-------------|
| `misc_yards` | INTEGER | Miscellaneous yards |
| `fumble_recovery_own` | INTEGER | Own fumble recoveries |
| `fumble_recovery_yards_own` | INTEGER | Own fumble recovery yards |
| `fumble_recovery_opp` | INTEGER | Opponent fumble recoveries |
| `fumble_recovery_yards_opp` | INTEGER | Opponent fumble recovery yards |
| `fumble_recovery_tds` | INTEGER | Fumble recovery touchdowns |

#### Penalties (2 columns)

| Column | Type | Description |
|--------|------|-------------|
| `penalties` | INTEGER | Penalties committed |
| `penalty_yards` | INTEGER | Penalty yardage |

#### Kicking — Field Goals (24 columns)

| Column | Type | Description |
|--------|------|-------------|
| `fg_made` | INTEGER | Field goals made |
| `fg_att` | INTEGER | Field goal attempts |
| `fg_missed` | INTEGER | Field goals missed |
| `fg_blocked` | INTEGER | Field goals blocked |
| `fg_long` | REAL | Longest field goal |
| `fg_pct` | REAL | Field goal percentage |
| `fg_made_0_19` | INTEGER | FG made 0-19 yards |
| `fg_made_20_29` | INTEGER | FG made 20-29 yards |
| `fg_made_30_39` | INTEGER | FG made 30-39 yards |
| `fg_made_40_49` | INTEGER | FG made 40-49 yards |
| `fg_made_50_59` | INTEGER | FG made 50-59 yards |
| `fg_made_60_` | INTEGER | FG made 60+ yards |
| `fg_missed_0_19` | INTEGER | FG missed 0-19 yards |
| `fg_missed_20_29` | INTEGER | FG missed 20-29 yards |
| `fg_missed_30_39` | INTEGER | FG missed 30-39 yards |
| `fg_missed_40_49` | INTEGER | FG missed 40-49 yards |
| `fg_missed_50_59` | INTEGER | FG missed 50-59 yards |
| `fg_missed_60_` | INTEGER | FG missed 60+ yards |
| `fg_made_list` | TEXT | List of FG distances made |
| `fg_missed_list` | TEXT | List of FG distances missed |
| `fg_blocked_list` | TEXT | List of FG distances blocked |
| `fg_made_distance` | INTEGER | Total FG made distance |
| `fg_missed_distance` | INTEGER | Total FG missed distance |
| `fg_blocked_distance` | INTEGER | Total FG blocked distance |

#### Kicking — PATs & Game-Winners (10 columns)

| Column | Type | Description |
|--------|------|-------------|
| `pat_made` | INTEGER | PATs made |
| `pat_att` | INTEGER | PAT attempts |
| `pat_missed` | INTEGER | PATs missed |
| `pat_blocked` | INTEGER | PATs blocked |
| `pat_pct` | REAL | PAT percentage |
| `gwfg_made` | INTEGER | Game-winning FGs made |
| `gwfg_att` | INTEGER | Game-winning FG attempts |
| `gwfg_missed` | INTEGER | Game-winning FGs missed |
| `gwfg_blocked` | INTEGER | Game-winning FGs blocked |
| `gwfg_distance` | INTEGER | Game-winning FG distance |

#### Fantasy (2 columns)

| Column | Type | Description |
|--------|------|-------------|
| `fantasy_points` | REAL | Standard fantasy points |
| `fantasy_points_ppr` | REAL | PPR fantasy points |

#### Game Reference (1 column)

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | TEXT | nflverse game ID (only populated 2002+) |

**Notable v2 column name changes from legacy:**
- `player_id` (was `gsis_id`)
- `opponent_team` (was `opponent`)
- `attempts` (was `pass_attempts`)
- `passing_interceptions` (was `interceptions`)
- `sacks_suffered` (was `sacks`)
- `sack_yards_lost` (was `sack_yards`)

**Indexes:**
- `idx_game_stats_player_season` on `(player_id, season)`

---

### Table: `season_stats`

Aggregated season totals. Same stat columns as `game_stats` but summed across all weeks, minus `week`, `opponent_team`, and `game_id`, plus `recent_team` and `games`.

**Rows:** 49,489 | **Years:** 1999-2025 | **Columns:** 113

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | TEXT | **FK** → `players.gsis_id` (GSIS format) |
| `player_name` | TEXT | Player name |
| `player_display_name` | TEXT | Full display name |
| `position` | TEXT | Position |
| `position_group` | TEXT | Position group |
| `headshot_url` | TEXT | Headshot image URL |
| `season` | INTEGER | Season year |
| `season_type` | TEXT | REG or POST |
| `recent_team` | TEXT | Primary team for season |
| `games` | INTEGER | Games played |
| *(all 97 stat columns from game_stats — passing through kicking)* | | Summed totals |
| `fantasy_points` | REAL | Season fantasy points |
| `fantasy_points_ppr` | REAL | Season PPR fantasy points |

**Note:** `season_stats.recent_team` is backfilled from `game_stats.team` (most common team per player-season); nflverse source data doesn't always populate it.

**Note:** `season_stats` uses `gwfg_distance_list` (TEXT) instead of `game_stats`'s `gwfg_distance` (INTEGER).

**Indexes:**
- `idx_season_stats_player_season` on `(player_id, season)`

---

### Table: `draft_picks`

Historical NFL draft data with career statistics and accolades.

**Rows:** 12,670 | **Years:** 1980-2025 | **Columns:** 36

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Draft year |
| `round` | INTEGER | Draft round |
| `pick` | INTEGER | Overall pick number |
| `team` | TEXT | Drafting team |
| `gsis_id` | TEXT | Player GSIS ID (may be NULL for older picks) |
| `pfr_player_id` | TEXT | Pro Football Reference ID |
| `cfb_player_id` | TEXT | College Football Reference ID |
| `pfr_player_name` | TEXT | Player name |
| `hof` | INTEGER | 1 if Hall of Fame inductee |
| `position` | TEXT | Position drafted |
| `category` | TEXT | Position category |
| `side` | TEXT | Side of ball |
| `college` | TEXT | College |
| `age` | REAL | Age at draft |
| `to` | REAL | Last active season |
| `allpro` | INTEGER | All-Pro selections |
| `probowls` | INTEGER | Pro Bowl selections |
| `seasons_started` | INTEGER | Seasons as starter |
| `w_av` | REAL | Weighted Approximate Value |
| `car_av` | TEXT | Career Approximate Value |
| `dr_av` | REAL | Draft Approximate Value |
| `games` | REAL | Career games played |
| `pass_completions` | REAL | Career pass completions |
| `pass_attempts` | REAL | Career pass attempts |
| `pass_yards` | REAL | Career passing yards |
| `pass_tds` | REAL | Career passing TDs |
| `pass_ints` | REAL | Career interceptions thrown |
| `rush_atts` | REAL | Career rush attempts |
| `rush_yards` | REAL | Career rushing yards |
| `rush_tds` | REAL | Career rushing TDs |
| `receptions` | REAL | Career receptions |
| `rec_yards` | REAL | Career receiving yards |
| `rec_tds` | REAL | Career receiving TDs |
| `def_solo_tackles` | REAL | Career solo tackles |
| `def_ints` | REAL | Career defensive INTs |
| `def_sacks` | REAL | Career sacks |

**Notable v2 column name changes from legacy:**
- `pfr_player_id` (was `pfr_id`)
- `pfr_player_name` (was `player_name`)

---

### Table: `combine`

NFL Scouting Combine results with draft information.

**Rows:** 8,649 | **Years:** 2000-2025 | **Columns:** 18

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Combine year |
| `draft_year` | REAL | Draft year |
| `draft_team` | TEXT | Team that drafted player |
| `draft_round` | REAL | Draft round |
| `draft_ovr` | REAL | Overall draft pick |
| `pfr_id` | TEXT | Pro Football Reference ID |
| `cfb_id` | TEXT | College Football Reference ID |
| `player_name` | TEXT | Player name |
| `pos` | TEXT | Position |
| `school` | TEXT | College |
| `ht` | TEXT | Height (e.g., "6-2") |
| `wt` | REAL | Weight in pounds |
| `forty` | REAL | 40-yard dash time (seconds) |
| `bench` | REAL | Bench press reps (225 lbs) |
| `vertical` | REAL | Vertical jump (inches) |
| `broad_jump` | REAL | Broad jump (inches) |
| `cone` | REAL | 3-cone drill time (seconds) |
| `shuttle` | REAL | 20-yard shuttle time (seconds) |

**Notable v2 column name changes from legacy:**
- `pos` (was `position`)
- `ht` (was `height`)
- `wt` (was `weight`)

---

## Supplementary Tables

These tables provide advanced analytics from specialized data sources. Each uses different ID systems — see [ID System](#id-system--relationships) for join patterns.

### Table: `snap_counts`

Weekly snap participation by phase (offense, defense, special teams).

**Rows:** 276,948 | **Years:** 2015-2025 | **ID:** PFR ID (`pfr_player_id`) | **Columns:** 16

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | TEXT | nflverse game ID (e.g., `2024_01_KC_BAL`) |
| `pfr_game_id` | TEXT | PFR game ID |
| `season` | INTEGER | Season year |
| `game_type` | TEXT | REG or POST |
| `week` | INTEGER | Week number |
| `player` | TEXT | Player name |
| `pfr_player_id` | TEXT | **PFR ID** (e.g., `MahoPa00`) |
| `position` | TEXT | Position |
| `team` | TEXT | Team |
| `opponent` | TEXT | Opponent |
| `offense_snaps` | REAL | Offensive snap count |
| `offense_pct` | REAL | % of team offensive snaps |
| `defense_snaps` | REAL | Defensive snap count |
| `defense_pct` | REAL | % of team defensive snaps |
| `st_snaps` | REAL | Special teams snap count |
| `st_pct` | REAL | % of team special teams snaps |

---

### Table: `ngs_stats`

NFL Next Gen Stats advanced metrics. Contains three stat types with different columns populated.

**Rows:** 26,656 | **Years:** 2016-2025 | **ID:** GSIS ID (`player_gsis_id`) | **Columns:** 52

**Stat Types:** `passing`, `rushing`, `receiving`

#### Common Columns

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Season year |
| `season_type` | TEXT | REG or POST |
| `week` | INTEGER | Week (0 = season total) |
| `player_display_name` | TEXT | Player name |
| `player_gsis_id` | TEXT | **GSIS ID** |
| `player_position` | TEXT | Position |
| `team_abbr` | TEXT | Team |
| `player_first_name` | TEXT | First name |
| `player_last_name` | TEXT | Last name |
| `player_jersey_number` | INTEGER | Jersey number |
| `player_short_name` | TEXT | Short name (e.g., "P.Mahomes") |
| `stat_type` | TEXT | `passing`, `rushing`, or `receiving` |

#### Passing-Specific Columns

| Column | Type | Description |
|--------|------|-------------|
| `avg_time_to_throw` | REAL | Average time to throw (seconds) |
| `avg_completed_air_yards` | REAL | Average air yards on completions |
| `avg_intended_air_yards` | REAL | Average intended air yards |
| `avg_air_yards_differential` | REAL | Completed - Intended air yards |
| `aggressiveness` | REAL | % of tight-window throws |
| `max_completed_air_distance` | REAL | Longest completion (air distance) |
| `avg_air_yards_to_sticks` | REAL | Air yards relative to first down |
| `attempts` | REAL | Pass attempts |
| `pass_yards` | REAL | Passing yards |
| `pass_touchdowns` | REAL | Passing TDs |
| `interceptions` | REAL | Interceptions |
| `passer_rating` | REAL | Passer rating |
| `completions` | REAL | Completions |
| `completion_percentage` | REAL | Completion % |
| `expected_completion_percentage` | REAL | Expected completion % |
| `completion_percentage_above_expectation` | REAL | CPOE |
| `avg_air_distance` | REAL | Average throw distance |
| `max_air_distance` | REAL | Max throw distance |

#### Rushing-Specific Columns

| Column | Type | Description |
|--------|------|-------------|
| `efficiency` | REAL | Rushing efficiency |
| `percent_attempts_gte_eight_defenders` | REAL | % runs vs 8+ defenders |
| `avg_time_to_los` | REAL | Avg time to line of scrimmage |
| `rush_attempts` | REAL | Rush attempts |
| `rush_yards` | REAL | Rush yards |
| `avg_rush_yards` | REAL | Yards per carry |
| `rush_touchdowns` | REAL | Rush TDs |
| `expected_rush_yards` | REAL | Expected rush yards |
| `rush_yards_over_expected` | REAL | RYOE total |
| `rush_yards_over_expected_per_att` | REAL | RYOE per attempt |
| `rush_pct_over_expected` | REAL | % over expected |

#### Receiving-Specific Columns

| Column | Type | Description |
|--------|------|-------------|
| `avg_cushion` | REAL | Avg separation at snap |
| `avg_separation` | REAL | Avg separation at catch |
| `percent_share_of_intended_air_yards` | REAL | Air yards share |
| `receptions` | REAL | Receptions |
| `targets` | REAL | Targets |
| `catch_percentage` | REAL | Catch % |
| `yards` | REAL | Receiving yards |
| `rec_touchdowns` | REAL | Receiving TDs |
| `avg_yac` | REAL | Avg yards after catch |
| `avg_expected_yac` | REAL | Expected YAC |
| `avg_yac_above_expectation` | REAL | YAC over expected |

---

### Table: `depth_charts`

Weekly depth chart positions for all teams (historical).

**Rows:** 869,185 | **Years:** 2001-2024 | **ID:** GSIS ID (`gsis_id`) | **Columns:** 15

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Season year |
| `club_code` | TEXT | Team abbreviation |
| `week` | REAL | Week number |
| `game_type` | TEXT | REG or POST |
| `depth_team` | TEXT | Depth (1 = starter, 2 = backup, etc.) |
| `last_name` | TEXT | Last name |
| `first_name` | TEXT | First name |
| `football_name` | TEXT | Football name |
| `formation` | TEXT | Offense or Defense |
| `gsis_id` | TEXT | **GSIS ID** |
| `jersey_number` | TEXT | Jersey number |
| `position` | TEXT | Listed position |
| `elias_id` | TEXT | Elias Sports Bureau ID |
| `depth_position` | TEXT | Specific depth position (e.g., "RCB", "LT") |
| `full_name` | TEXT | Full name |

---

### Table: `depth_charts_2025`

Daily depth chart positions for 2025+ season. **Different schema from `depth_charts`** — nflverse changed the depth chart format starting in 2025.

**Rows:** 476,501 | **Date range:** 2025-08-03 to 2026-02-13 | **ID:** GSIS ID (`gsis_id`) + ESPN ID (`espn_id`) | **Columns:** 12

| Column | Type | Description |
|--------|------|-------------|
| `dt` | TEXT | Date (YYYY-MM-DD) — no `season` column |
| `team` | TEXT | Team abbreviation |
| `player_name` | TEXT | Player name |
| `espn_id` | TEXT | ESPN player ID |
| `gsis_id` | TEXT | **GSIS ID** |
| `pos_grp_id` | TEXT | Position group ID |
| `pos_grp` | TEXT | Position group name |
| `pos_id` | TEXT | Position ID |
| `pos_name` | TEXT | Position name |
| `pos_abb` | TEXT | Position abbreviation |
| `pos_slot` | INTEGER | Position slot number |
| `pos_rank` | INTEGER | Depth chart rank (1 = starter) |

**Key differences from `depth_charts`:**
- Uses `dt` (date) instead of `season` + `week` — daily snapshots rather than weekly
- No `formation`, `depth_position`, or `elias_id` columns
- Includes `espn_id` for direct ESPN joins
- Position info split into `pos_grp`/`pos_name`/`pos_abb` instead of `position`/`depth_position`

---

### Table: `pfr_advanced`

Pro Football Reference advanced statistics. Contains three stat types with different columns.

**Rows:** 7,798 | **Years:** 2018-2025 | **ID:** PFR ID (`pfr_id`) | **Columns:** 64

**Stat Types:** `pass`, `rush`, `rec`

#### Common Columns

| Column | Type | Description |
|--------|------|-------------|
| `player` | TEXT | Player name |
| `team` | TEXT | Team |
| `season` | INTEGER | Season year |
| `pfr_id` | TEXT | **PFR ID** (e.g., `RoetBe00`) |
| `stat_type` | TEXT | `pass`, `rush`, or `rec` |
| `tm` | TEXT | Team abbreviation |
| `age` | REAL | Age |
| `pos` | TEXT | Position |
| `g` | REAL | Games played |
| `gs` | REAL | Games started |
| `loaded` | TIMESTAMP | Data load timestamp |

#### Passing-Specific Columns (`stat_type = 'pass'`)

| Column | Type | Description |
|--------|------|-------------|
| `pass_attempts` | REAL | Pass attempts |
| `throwaways` | REAL | Throwaways |
| `spikes` | REAL | Spikes |
| `drops` | REAL | Drops by receivers |
| `drop_pct` | REAL | Drop % |
| `bad_throws` | REAL | Bad throws |
| `bad_throw_pct` | REAL | Bad throw % |
| `pocket_time` | REAL | Avg time in pocket (seconds) |
| `times_blitzed` | REAL | Times blitzed |
| `times_hurried` | REAL | Times hurried |
| `times_hit` | REAL | Times hit |
| `times_pressured` | REAL | Times pressured |
| `pressure_pct` | REAL | Pressure % |
| `batted_balls` | REAL | Batted passes |
| `on_tgt_throws` | REAL | On-target throws |
| `on_tgt_pct` | REAL | On-target % |
| `rpo_plays` | REAL | RPO plays |
| `rpo_yards` | REAL | RPO yards |
| `rpo_pass_att` | REAL | RPO pass attempts |
| `rpo_pass_yards` | REAL | RPO pass yards |
| `rpo_rush_att` | REAL | RPO rush attempts |
| `rpo_rush_yards` | REAL | RPO rush yards |
| `pa_pass_att` | REAL | Play-action pass attempts |
| `pa_pass_yards` | REAL | Play-action pass yards |
| `intended_air_yards` | REAL | Total intended air yards |
| `intended_air_yards_per_pass_attempt` | REAL | IAY per attempt |
| `completed_air_yards` | REAL | Total completed air yards |
| `completed_air_yards_per_completion` | REAL | CAY per completion |
| `completed_air_yards_per_pass_attempt` | REAL | CAY per attempt |
| `pass_yards_after_catch` | REAL | Total YAC |
| `pass_yards_after_catch_per_completion` | REAL | YAC per completion |
| `scrambles` | REAL | Scrambles |
| `scramble_yards_per_attempt` | REAL | Yards per scramble |

#### Rushing-Specific Columns (`stat_type = 'rush'`)

| Column | Type | Description |
|--------|------|-------------|
| `att` | REAL | Rush attempts |
| `yds` | REAL | Rush yards |
| `td` | REAL | Rush TDs |
| `x1d` | REAL | First downs |
| `ybc` | REAL | Yards before contact |
| `ybc_att` | REAL | YBC per attempt |
| `yac` | REAL | Yards after contact |
| `yac_att` | REAL | YAC per attempt |
| `brk_tkl` | REAL | Broken tackles |
| `att_br` | REAL | Attempts per broken tackle |

#### Receiving-Specific Columns (`stat_type = 'rec'`)

| Column | Type | Description |
|--------|------|-------------|
| `tgt` | REAL | Targets |
| `rec` | REAL | Receptions |
| `yds` | REAL | Receiving yards |
| `td` | REAL | Receiving TDs |
| `x1d` | REAL | First downs |
| `ybc_r` | REAL | Yards before catch |
| `yac_r` | REAL | Yards after catch |
| `adot` | REAL | Average depth of target |
| `brk_tkl` | REAL | Broken tackles |
| `rec_br` | REAL | Receptions per broken tackle |
| `drop` | REAL | Drops |
| `drop_percent` | REAL | Drop % |
| `int` | REAL | INTs on targets |
| `rat` | REAL | Passer rating when targeted |

---

### Table: `qbr`

ESPN Total QBR (Quarterback Rating) data. Includes weekly and season totals.

**Rows:** 9,570 | **Years:** 2006-2023 | **ID:** ESPN ID (`player_id`) | **Columns:** 30

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Season year |
| `season_type` | TEXT | "Regular" or "Postseason" |
| `game_id` | INTEGER | ESPN game ID |
| `game_week` | INTEGER | Week number |
| `week_text` | TEXT | Week label (e.g., "Week 1", "Season Total") |
| `team_abb` | TEXT | Team abbreviation |
| `player_id` | INTEGER | **ESPN ID** |
| `name_short` | TEXT | Short name (e.g., "P. Manning") |
| `rank` | REAL | QBR rank |
| `qbr_total` | REAL | Total QBR (0-100 scale) |
| `pts_added` | REAL | Points added above average |
| `qb_plays` | INTEGER | Total QB plays |
| `epa_total` | REAL | Total EPA |
| `pass` | REAL | Pass EPA |
| `run` | REAL | Run EPA |
| `exp_sack` | INTEGER | Expected sacks |
| `penalty` | REAL | Penalty EPA |
| `qbr_raw` | REAL | Raw QBR |
| `sack` | REAL | Sack EPA |
| `name_first` | TEXT | First name |
| `name_last` | TEXT | Last name |
| `name_display` | TEXT | Display name |
| `headshot_href` | TEXT | ESPN headshot URL |
| `team` | TEXT | Team name |
| `opp_id` | INTEGER | Opponent ESPN team ID |
| `opp_abb` | TEXT | Opponent abbreviation |
| `opp_team` | TEXT | Opponent team name |
| `opp_name` | TEXT | Opponent name |
| `week_num` | INTEGER | Week number (numeric) |
| `qualified` | INTEGER | 1 if qualified for rankings |

---

## Play-by-Play Database (`pbp_v2.db`)

Separate database containing every NFL play from 1999-2025. At 2,082 MB it is too large to combine with the main database.

### Table: `play_by_play`

**Rows:** 1,279,628 | **Years:** 1999-2025 | **Columns:** 372

All player ID columns use **GSIS ID** format (`00-0035228`) for direct joins to `nflverse_v2.db.players.gsis_id`.

v2 retains all 372 nflverse columns (the legacy `pbp.db` trimmed these to 84). The columns are organized into the following categories:

#### Game/Play Identification (~15 columns)

`play_id`, `game_id`, `old_game_id`, `home_team`, `away_team`, `season_type`, `week`, `season`, `game_date`, `posteam`, `posteam_type`, `defteam`, `side_of_field`, `yardline_100`

#### Timing & Situation (~15 columns)

`game_half`, `quarter_seconds_remaining`, `half_seconds_remaining`, `game_seconds_remaining`, `qtr`, `down`, `goal_to_go`, `time`, `yrdln`, `ydstogo`, `ydsnet`, `quarter_end`

#### Play Description & Type (~10 columns)

`desc`, `play_type`, `play_type_nfl`, `yards_gained`, `shotgun`, `no_huddle`, `qb_dropback`, `qb_kneel`, `qb_spike`, `qb_scramble`

#### Passing Details (~5 columns)

`pass_length`, `pass_location`, `air_yards`, `yards_after_catch`, `passing_yards`

#### Rushing Details (~3 columns)

`run_location`, `run_gap`, `rushing_yards`

#### Play Outcome Flags (~30 columns)

`first_down`, `rush_attempt`, `pass_attempt`, `complete_pass`, `incomplete_pass`, `sack`, `touchdown`, `pass_touchdown`, `rush_touchdown`, `return_touchdown`, `interception`, `fumble`, `fumble_forced`, `fumble_not_forced`, `fumble_lost`, `fumble_out_of_bounds`, `solo_tackle`, `assist_tackle`, `tackle_with_assist`, `tackled_for_loss`, `qb_hit`, `safety`, `penalty`, `punt_blocked`, `lateral_reception`, `lateral_rush`, `lateral_return`, `lateral_recovery`, `aborted_play`, `success`

#### Scoring (~15 columns)

`posteam_score`, `defteam_score`, `score_differential`, `total_home_score`, `total_away_score`, `posteam_score_post`, `defteam_score_post`, `score_differential_post`, `sp`, `td_team`, `td_player_name`, `td_player_id`

#### Field Goals, Extra Points & 2PT (~10 columns)

`field_goal_attempt`, `field_goal_result`, `kick_distance`, `extra_point_attempt`, `extra_point_result`, `extra_point_prob`, `two_point_attempt`, `two_point_conv_result`, `two_point_conversion_prob`, `defensive_two_point_attempt`, `defensive_two_point_conv`, `defensive_extra_point_attempt`, `defensive_extra_point_conv`

#### Timeouts (~5 columns)

`timeout`, `timeout_team`, `home_timeouts_remaining`, `away_timeouts_remaining`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`

#### Player IDs (~100 columns)

All player involvement tracked with `*_player_id` + `*_player_name` pairs:
- Primary: `passer_*`, `rusher_*`, `receiver_*`, `kicker_*`, `punter_*`
- Defensive: `interception_*`, `sack_*`, `half_sack_1_*`, `half_sack_2_*`, `qb_hit_1_*`, `qb_hit_2_*`
- Tackles: `solo_tackle_1_*`, `solo_tackle_2_*`, `assist_tackle_1_*` through `assist_tackle_4_*`, `tackle_with_assist_1_*`, `tackle_with_assist_2_*`, `tackle_for_loss_1_*`, `tackle_for_loss_2_*`
- Fumbles: `fumbled_1_*`, `fumbled_2_*`, `fumble_recovery_1_*`, `fumble_recovery_2_*`, `forced_fumble_player_1_*`, `forced_fumble_player_2_*`
- Returns: `punt_returner_*`, `kickoff_returner_*`, plus lateral variants
- Special: `pass_defense_1_*`, `pass_defense_2_*`, `blocked_*`, `penalty_*`, `safety_*`, `own_kickoff_recovery_*`
- Fantasy: `fantasy_player_id`, `fantasy_player_name`, `fantasy`, `fantasy_id`
- Shorthand: `passer`, `rusher`, `receiver`, `passer_id`, `rusher_id`, `receiver_id`, `name`, `id`

#### Expected Points & Win Probability (~50 columns)

Core metrics: `ep`, `epa`, `wp`, `def_wp`, `home_wp`, `away_wp`, `wpa`, `vegas_wpa`, `vegas_wp`, `vegas_home_wp`, `vegas_home_wpa`

Detailed breakdowns: `air_epa`, `yac_epa`, `comp_air_epa`, `comp_yac_epa`, `air_wpa`, `yac_wpa`, `comp_air_wpa`, `comp_yac_wpa`

Team totals: `total_home_epa`, `total_away_epa`, `total_home_rush_epa`, `total_away_rush_epa`, `total_home_pass_epa`, `total_away_pass_epa`, etc.

Scoring probabilities: `no_score_prob`, `opp_fg_prob`, `opp_safety_prob`, `opp_td_prob`, `fg_prob`, `safety_prob`, `td_prob`

Completion: `cp`, `cpoe`

xYAC: `xyac_epa`, `xyac_mean_yardage`, `xyac_median_yardage`, `xyac_success`, `xyac_fd`

Pass over expected: `xpass`, `pass_oe`, `qb_epa`

#### Punt/Kickoff Details (~15 columns)

`punt_inside_twenty`, `punt_in_endzone`, `punt_out_of_bounds`, `punt_downed`, `punt_fair_catch`, `kickoff_inside_twenty`, `kickoff_in_endzone`, `kickoff_out_of_bounds`, `kickoff_downed`, `kickoff_fair_catch`, `own_kickoff_recovery`, `own_kickoff_recovery_td`, `touchback`

#### Penalty Details (~5 columns)

`penalty`, `penalty_team`, `penalty_player_id`, `penalty_player_name`, `penalty_yards`, `penalty_type`

#### Conversion Details (~5 columns)

`first_down_rush`, `first_down_pass`, `first_down_penalty`, `third_down_converted`, `third_down_failed`, `fourth_down_converted`, `fourth_down_failed`

#### Drive Info (~20 columns)

`drive`, `fixed_drive`, `fixed_drive_result`, `drive_play_count`, `drive_time_of_possession`, `drive_first_downs`, `drive_inside20`, `drive_ended_with_score`, `drive_quarter_start`, `drive_quarter_end`, `drive_yards_penalized`, `drive_start_transition`, `drive_end_transition`, `drive_game_clock_start`, `drive_game_clock_end`, `drive_start_yard_line`, `drive_end_yard_line`, `drive_play_id_started`, `drive_play_id_ended`, `drive_real_start_time`

#### Game Info (~20 columns)

`away_score`, `home_score`, `location`, `result`, `total`, `spread_line`, `total_line`, `div_game`, `roof`, `surface`, `temp`, `wind`, `home_coach`, `away_coach`, `stadium_id`, `game_stadium`, `stadium`

#### Metadata (~10 columns)

`series`, `series_success`, `series_result`, `order_sequence`, `start_time`, `time_of_day`, `weather`, `nfl_api_id`, `play_clock`, `play_deleted`, `special_teams_play`, `st_play_type`, `end_clock_time`, `end_yard_line`, `replay_or_challenge`, `replay_or_challenge_result`, `out_of_bounds`, `home_opening_kickoff`

**Note:** `pbp_v2.db` has no indexes. Consider adding indexes on frequently-queried columns for better performance.

---

## Indexes

v2 databases use minimal indexes. Only 3 indexes exist:

| Index | Table | Columns |
|-------|-------|---------|
| `idx_game_stats_player_season` | `game_stats` | `(player_id, season)` |
| `idx_season_stats_player_season` | `season_stats` | `(player_id, season)` |
| `idx_players_gsis_id` | `players` | `gsis_id` |

`pbp_v2.db` has no indexes.

---

## Example Queries

### Get a player's career stats

```sql
SELECT
    p.display_name,
    s.season,
    s.games,
    s.passing_yards,
    s.passing_tds,
    s.rushing_yards,
    s.rushing_tds,
    s.fantasy_points
FROM season_stats s
JOIN players p ON s.player_id = p.gsis_id
WHERE p.display_name = 'Patrick Mahomes'
ORDER BY s.season;
```

### Find RBs with passing touchdowns (trick plays)

```sql
SELECT
    p.display_name,
    g.season,
    g.week,
    g.opponent_team,
    g.passing_yards,
    g.passing_tds
FROM game_stats g
JOIN players p ON g.player_id = p.gsis_id
WHERE p.position = 'RB' AND g.passing_tds > 0
ORDER BY g.passing_tds DESC
LIMIT 20;
```

### Top 10 fantasy seasons (all-time)

```sql
SELECT
    p.display_name,
    p.position,
    s.season,
    s.fantasy_points
FROM season_stats s
JOIN players p ON s.player_id = p.gsis_id
WHERE s.season_type = 'REG'
ORDER BY s.fantasy_points DESC
LIMIT 10;
```

### Top defensive players by sacks (single season)

```sql
SELECT
    p.display_name,
    s.season,
    s.def_sacks,
    s.def_qb_hits,
    s.def_tackles_for_loss,
    s.recent_team
FROM season_stats s
JOIN players p ON s.player_id = p.gsis_id
WHERE s.season_type = 'REG' AND s.def_sacks > 0
ORDER BY s.def_sacks DESC
LIMIT 20;
```

### Get player's snap counts with snap share

```sql
SELECT
    p.display_name,
    sc.week,
    sc.offense_snaps,
    sc.offense_pct,
    sc.team
FROM snap_counts sc
JOIN player_ids pi ON sc.pfr_player_id = pi.pfr_id
JOIN players p ON pi.gsis_id = p.gsis_id
WHERE p.display_name = 'Justin Jefferson'
  AND sc.season = 2024
ORDER BY sc.week;
```

### Get QBR with player info

```sql
SELECT
    p.display_name,
    p.latest_team,
    q.qbr_total,
    q.pts_added,
    q.qb_plays
FROM qbr q
JOIN player_ids pi ON q.player_id = pi.espn_id
JOIN players p ON pi.gsis_id = p.gsis_id
WHERE q.season = 2023
  AND q.week_text = 'Season Total'
ORDER BY q.qbr_total DESC
LIMIT 10;
```

### Kicker season stats

```sql
SELECT
    p.display_name,
    s.season,
    s.fg_made,
    s.fg_att,
    s.fg_pct,
    s.fg_long,
    s.pat_made,
    s.pat_att,
    s.recent_team
FROM season_stats s
JOIN players p ON s.player_id = p.gsis_id
WHERE p.position = 'K' AND s.season = 2024 AND s.season_type = 'REG'
ORDER BY s.fg_made DESC;
```

### Hall of Fame draft picks with career stats

```sql
SELECT
    pfr_player_name,
    season AS draft_year,
    round,
    pick,
    team,
    position,
    probowls,
    allpro,
    games,
    pass_yards,
    rush_yards,
    rec_yards,
    def_sacks
FROM draft_picks
WHERE hof = 1
ORDER BY season;
```

### Red zone efficiency from play-by-play

```sql
-- Attach pbp_v2.db when needed
ATTACH DATABASE 'pbp_v2.db' AS pbp;

SELECT
    p.passer_player_name,
    COUNT(*) as red_zone_passes,
    SUM(p.touchdown) as pass_tds,
    ROUND(100.0 * SUM(p.touchdown) / COUNT(*), 1) as td_rate
FROM pbp.play_by_play p
WHERE p.season = 2024
  AND p.yardline_100 <= 20
  AND p.pass_attempt = 1
  AND p.passer_player_id IS NOT NULL
GROUP BY p.passer_player_id, p.passer_player_name
HAVING COUNT(*) >= 20
ORDER BY td_rate DESC
LIMIT 10;
```

### Cross-reference player IDs

```sql
SELECT
    p.display_name,
    i.espn_id,
    i.yahoo_id,
    i.sleeper_id,
    i.pfr_id,
    i.fantasy_data_id
FROM players p
JOIN player_ids i ON p.gsis_id = i.gsis_id
WHERE p.display_name = 'Justin Jefferson';
```

---

## Fantasy Points Calculation

nflverse pre-calculates fantasy points using standard scoring:

**Standard (non-PPR):**
- Passing: 0.04 pts/yard, 4 pts/TD, -2 pts/INT
- Rushing: 0.1 pts/yard, 6 pts/TD
- Receiving: 0.1 pts/yard, 6 pts/TD
- 2PT Conversion: 2 pts
- Fumble Lost: -2 pts

**PPR (Points Per Reception):**
- Same as standard + 1 pt/reception

---

## Data Source

Data is sourced from [nflverse](https://github.com/nflverse) via `nflreadpy` (successor to the archived `nfl_data_py`).

---

## Build Scripts

```bash
# Full build from scratch (all tables, all years)
python3 scripts/update_db.py --all

# Full build to a specific output file
python3 scripts/update_db.py --all --output data/nflverse_v2.db

# Incremental updates
python3 scripts/update_db.py --years 2025
python3 scripts/update_db.py --tables game_stats players
python3 scripts/update_db.py --pbp --years 2025

# Play-by-play (separate DB)
python3 scripts/update_db.py --pbp --all
```

---

## Notes

- **Column naming**: All tables use nflverse-native column names. Legacy DBs (`nflverse_custom.db`, `pbp.db`) used custom renames — do not mix.
- **All position groups**: `game_stats`/`season_stats` include ~115 columns covering all positions (offensive, defensive, kicking, special teams). ~3x more rows than legacy (all position groups, not just skill positions).
- **`season_stats.recent_team`**: Backfilled from `game_stats.team` (most common team per player-season); nflverse source doesn't always populate it.
- **`game_id` in `game_stats`**: Only populated for 2002+ (nflverse doesn't provide it for 1999-2001).
- **`depth_charts` vs `depth_charts_2025`**: Separate tables due to nflverse schema change in 2025. The 2025+ format uses daily snapshots (`dt` column) instead of weekly, and has a different position structure.
- **`combine` table**: Has no join edges to other tables — query separately.
- **NGS `stat_type`**: `passing`, `rushing`, `receiving`; `week=0` = season totals.
- **PFR `stat_type`**: `pass`, `rush`, `rec` (different naming from NGS!).
- **QBR**: `season_type` is `"Regular"` or `"Postseason"`; use `week_text` for "Season Total" filtering.
- **Schema drift**: Handled automatically by `scripts/update_db.py` which adds missing columns via `ALTER TABLE`.
- **Join path**: `game_stats.player_id = players.gsis_id` (same GSIS format, different column names).
- **`nfl_data_py`**: Archived Sept 2025. Successor is `nflreadpy`.
- **Draft picks**: Go back to 1980 with career stats, Pro Bowl/All-Pro counts, and HOF flag.
