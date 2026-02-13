# nflverse Database Documentation

Comprehensive NFL player statistics database built from [nflverse](https://github.com/nflverse) data via the `nfl_data_py` Python package (archived Sept 2025; successor is `nflreadpy`).

## Quick Stats

| Database | Size | Tables | Total Rows | Years |
|----------|------|--------|------------|-------|
| `nflverse_custom.db` | 186 MB | 12 | ~1.35M | 1999-2025 |
| `pbp.db` | 535 MB | 1 | ~1.23M | 1999-2024 |

### Table Row Counts

| Table | Rows | Description |
|-------|------|-------------|
| **players** | 24,356 | Master player registry (all positions) |
| **player_ids** | 7,705 | Cross-platform ID mapping (ESPN, Yahoo, PFR, etc.) |
| **games** | 7,276 | Schedule, scores, weather, betting |
| **game_stats** | 144,703 | Weekly player stats (48 columns) |
| **season_stats** | 15,704 | Aggregated season totals |
| **draft_picks** | 12,670 | Historical NFL draft data |
| **combine** | 8,649 | Scouting Combine results |
| **snap_counts** | 250,336 | Weekly snap participation (2015-2024) |
| **ngs_stats** | 24,068 | Next Gen Stats (2016-2024) |
| **depth_charts** | 869,185 | Weekly depth charts (2001-2024) |
| **pfr_advanced** | 6,820 | PFR advanced stats (2018-2024) |
| **qbr** | 1,363 | ESPN Total QBR (2006-2023) |
| **play_by_play** | 1,230,857 | Every NFL play (pbp.db) |

---

## Schema Overview

```
                          nflverse_custom.db
┌──────────────────────────────────────────────────────────────────────────┐
│                              PLAYERS                                      │
│     gsis_id (PK) | display_name | position | team | headshot_url | ...   │
└──────────────────────────────────────────────────────────────────────────┘
         │
         │ FK: gsis_id
         ▼
┌─────────────────────────┐        ┌─────────────────────────┐
│       GAME_STATS        │        │      SEASON_STATS       │
│  gsis_id + season +     │        │  gsis_id + season       │
│  week (UNIQUE)          │        │  (UNIQUE)               │
│  ALL 48 stat columns    │        │  Aggregated totals      │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│       PLAYER_IDS        │        │          GAMES          │
│  Cross-reference to     │        │  Schedule, scores,      │
│  ESPN, Yahoo, PFR...    │        │  weather, betting       │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│      DRAFT_PICKS        │        │        COMBINE          │
│  Historical drafts      │        │  40 time, bench, etc    │
└─────────────────────────┘        └─────────────────────────┘

                    SUPPLEMENTARY TABLES (Advanced Analytics)

┌─────────────────────────┐        ┌─────────────────────────┐
│      SNAP_COUNTS        │        │        NGS_STATS        │
│  Offense/Def/ST snaps   │        │  Next Gen Stats         │
│  Uses PFR ID            │        │  Uses GSIS ID           │
│  2015-2024              │        │  2016-2024              │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐        ┌─────────────────────────┐
│      DEPTH_CHARTS       │        │      PFR_ADVANCED       │
│  Weekly positions       │        │  Pressure, drops, etc   │
│  Uses GSIS ID           │        │  Uses PFR ID            │
│  2001-2024              │        │  2018-2024              │
└─────────────────────────┘        └─────────────────────────┘

┌─────────────────────────┐
│          QBR            │
│  ESPN Total QBR         │
│  Uses ESPN ID           │
│  2006-2023              │
└─────────────────────────┘

                              pbp.db (separate)
┌──────────────────────────────────────────────────────────────────────────┐
│                           PLAY_BY_PLAY                                    │
│     game_id + play_id | 83 columns | EPA/WPA | Player IDs (GSIS)         │
│     1.23M plays (1999-2024)                                              │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## ID System & Relationships

### Primary Key: GSIS ID

Most tables use the NFL's **Game Statistics & Information System ID (GSIS ID)** as the primary identifier:
- Format: `00-0033873` (Patrick Mahomes)
- Used in: `players`, `game_stats`, `season_stats`, `ngs_stats`, `depth_charts`, `pbp.db`

### Supplementary Table ID Mapping

Different data sources use different ID systems. The `player_ids` table provides cross-references:

| Table | ID Column | Format | Join Path |
|-------|-----------|--------|-----------|
| `ngs_stats` | `player_gsis_id` | `00-0035228` | Direct to `players.gsis_id` |
| `depth_charts` | `gsis_id` | `00-0035228` | Direct to `players.gsis_id` |
| `snap_counts` | `pfr_player_id` | `MahoPa00` | Via `player_ids.pfr_id` → `gsis_id` |
| `pfr_advanced` | `pfr_id` | `MahoPa00` | Via `player_ids.pfr_id` → `gsis_id` |
| `qbr` | `player_id` | `3139477` (ESPN) | Via `player_ids.espn_id` → `gsis_id` |
| `pbp.db` | `*_player_id` | `00-0035228` | Direct to `players.gsis_id` |

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

**Rows:** 24,356 | **Years:** 1999-2025

| Column | Type | Description |
|--------|------|-------------|
| `gsis_id` | TEXT | **Primary Key** - NFL GSIS ID (`00-0033873`) |
| `display_name` | TEXT | Full display name (e.g., "Patrick Mahomes") |
| `first_name` | TEXT | First name |
| `last_name` | TEXT | Last name |
| `position` | TEXT | Position (QB, RB, WR, TE, K, P, OL, DL, LB, DB, etc.) |
| `position_group` | TEXT | Position group (QB, RB, WR, TE, OL, DL, LB, DB, SPEC) |
| `current_team` | TEXT | Current team abbreviation (or last team if retired) |
| `jersey_number` | TEXT | Jersey number |
| `height` | REAL | Height in inches |
| `weight` | REAL | Weight in pounds |
| `birth_date` | TEXT | Birth date (YYYY-MM-DD) |
| `college` | TEXT | College attended |
| `college_conference` | TEXT | College conference |
| `rookie_season` | INTEGER | First NFL season |
| `last_season` | INTEGER | Most recent NFL season |
| `years_of_experience` | INTEGER | Years in NFL |
| `status` | TEXT | Current status (ACT, RES, CUT, etc.) |
| `headshot_url` | TEXT | URL to NFL CDN headshot image |
| `pfr_id` | TEXT | Pro Football Reference ID |
| `espn_id` | TEXT | ESPN player ID |
| `pff_id` | TEXT | Pro Football Focus ID |
| `draft_year` | REAL | Year drafted |
| `draft_round` | REAL | Draft round |
| `draft_pick` | REAL | Overall draft pick |
| `draft_team` | TEXT | Team that drafted player |

**Example Row (Patrick Mahomes):**
```
gsis_id: 00-0033873
display_name: Patrick Mahomes
position: QB
current_team: KC
height: 74.0 (6'2")
weight: 225.0
college: Texas Tech
rookie_season: 2017
headshot_url: https://static.www.nfl.com/image/upload/f_auto,q_auto/league/iireqbn32cpg9fn4sfy7
pfr_id: MahoPa00
espn_id: 3139477
draft_year: 2017, draft_round: 1, draft_pick: 10
```

**Indexes:**
- `idx_players_name` on `display_name`
- `idx_players_position` on `position`
- `idx_players_team` on `current_team`

---

### Table: `player_ids`

Cross-reference table mapping GSIS IDs to 18+ other platforms.

**Rows:** 7,705

| Column | Type | Description |
|--------|------|-------------|
| `gsis_id` | TEXT | **Primary Key** - NFL GSIS ID |
| `name` | TEXT | Player name |
| `position` | TEXT | Position |
| `team` | TEXT | Team |
| `espn_id` | REAL | ESPN ID |
| `yahoo_id` | REAL | Yahoo Fantasy ID |
| `fantasypros_id` | REAL | FantasyPros ID |
| `sleeper_id` | REAL | Sleeper app ID |
| `pfr_id` | TEXT | Pro Football Reference ID (format: `MahoPa00`) |
| `pff_id` | REAL | Pro Football Focus ID |
| `cbs_id` | REAL | CBS Sports ID |
| `rotowire_id` | REAL | Rotowire ID |
| `rotoworld_id` | REAL | Rotoworld ID |
| `fantasy_data_id` | REAL | FantasyData API ID |
| `sportradar_id` | TEXT | Sportradar UUID |
| `mfl_id` | INTEGER | MyFantasyLeague ID |
| `fleaflicker_id` | REAL | Fleaflicker ID |
| `stats_id` | REAL | Stats Inc ID |
| `stats_global_id` | REAL | Stats Global ID |
| `cfbref_id` | TEXT | College Football Reference ID |
| `nfl_id` | REAL | NFL.com ID |

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

NFL game schedule with scores, venue, weather, and betting information.

**Rows:** 7,276 | **Years:** 1999-2025

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
| `home_team` | TEXT | Home team abbreviation |
| `away_score` | REAL | Away team final score |
| `home_score` | REAL | Home team final score |
| `location` | TEXT | Game location |
| `result` | REAL | Result from home team perspective |
| `total` | REAL | Total points scored |
| `overtime` | REAL | 1 if overtime, 0 otherwise |
| `spread_line` | REAL | Point spread (home team) |
| `total_line` | REAL | Over/under line |
| `away_moneyline` | REAL | Away team moneyline |
| `home_moneyline` | REAL | Home team moneyline |
| `away_rest` | INTEGER | Days rest for away team |
| `home_rest` | INTEGER | Days rest for home team |
| `stadium` | TEXT | Stadium name |
| `stadium_id` | TEXT | Stadium ID |
| `roof` | TEXT | Roof type (outdoors, dome, open, closed) |
| `surface` | TEXT | Playing surface |
| `temp` | REAL | Temperature (F) |
| `wind` | REAL | Wind speed (mph) |
| `away_coach` | TEXT | Away team head coach |
| `home_coach` | TEXT | Home team head coach |
| `referee` | TEXT | Head referee |

**Indexes:**
- `idx_games_season` on `season`
- `idx_games_week` on `(season, week)`
- `idx_games_teams` on `(home_team, away_team)`

---

### Table: `game_stats`

Weekly player statistics. **This is the main stats table.**

Unlike position-specific tables in older schemas, this unified table contains ALL stat columns for ALL positions. RBs can have passing stats (trick plays), WRs can have rushing stats (jet sweeps), etc.

**Rows:** 144,703 | **Years:** 1999-2025

#### Identification Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-increment primary key |
| `gsis_id` | TEXT | **FK** → `players.gsis_id` (NOT NULL) |
| `season` | INTEGER | Season year (NOT NULL) |
| `week` | INTEGER | Week number (NOT NULL) |
| `season_type` | TEXT | REG or POST (default: REG) |
| `team` | TEXT | Player's team for this game |
| `opponent` | TEXT | Opponent team |

#### Passing Stats (14 columns)

| Column | Type | Description |
|--------|------|-------------|
| `completions` | INTEGER | Completions |
| `pass_attempts` | INTEGER | Pass attempts |
| `passing_yards` | REAL | Passing yards |
| `passing_tds` | INTEGER | Passing touchdowns |
| `interceptions` | INTEGER | Interceptions thrown |
| `sacks` | INTEGER | Times sacked |
| `sack_yards` | REAL | Yards lost to sacks |
| `sack_fumbles` | INTEGER | Fumbles on sacks |
| `sack_fumbles_lost` | INTEGER | Fumbles lost on sacks |
| `passing_air_yards` | REAL | Air yards (depth of target) |
| `passing_yards_after_catch` | REAL | YAC on completions |
| `passing_first_downs` | REAL | First downs via passing |
| `passing_epa` | REAL | Expected Points Added (passing) |
| `passing_2pt_conversions` | INTEGER | 2-point conversions (passing) |

#### Rushing Stats (8 columns)

| Column | Type | Description |
|--------|------|-------------|
| `carries` | INTEGER | Rush attempts |
| `rushing_yards` | REAL | Rushing yards |
| `rushing_tds` | INTEGER | Rushing touchdowns |
| `rushing_fumbles` | INTEGER | Fumbles on rushes |
| `rushing_fumbles_lost` | INTEGER | Fumbles lost on rushes |
| `rushing_first_downs` | REAL | First downs via rushing |
| `rushing_epa` | REAL | Expected Points Added (rushing) |
| `rushing_2pt_conversions` | INTEGER | 2-point conversions (rushing) |

#### Receiving Stats (11 columns)

| Column | Type | Description |
|--------|------|-------------|
| `targets` | INTEGER | Times targeted |
| `receptions` | INTEGER | Receptions |
| `receiving_yards` | REAL | Receiving yards |
| `receiving_tds` | INTEGER | Receiving touchdowns |
| `receiving_fumbles` | INTEGER | Fumbles after catch |
| `receiving_fumbles_lost` | INTEGER | Fumbles lost after catch |
| `receiving_air_yards` | REAL | Air yards on targets |
| `receiving_yards_after_catch` | REAL | Yards after catch |
| `receiving_first_downs` | REAL | First downs via receiving |
| `receiving_epa` | REAL | Expected Points Added (receiving) |
| `receiving_2pt_conversions` | INTEGER | 2-point conversions (receiving) |

#### Advanced Metrics (6 columns)

| Column | Type | Description |
|--------|------|-------------|
| `target_share` | REAL | % of team targets |
| `air_yards_share` | REAL | % of team air yards |
| `wopr` | REAL | Weighted Opportunity Rating |
| `racr` | REAL | Receiver Air Conversion Ratio |
| `pacr` | REAL | Passer Air Conversion Ratio |
| `dakota` | REAL | Adjusted completion % metric |

#### Special Teams & Fantasy (3 columns)

| Column | Type | Description |
|--------|------|-------------|
| `special_teams_tds` | INTEGER | Return touchdowns |
| `fantasy_points` | REAL | Standard fantasy points |
| `fantasy_points_ppr` | REAL | PPR fantasy points |

**Constraints:**
- `UNIQUE(gsis_id, season, week)`
- `FOREIGN KEY (gsis_id) REFERENCES players(gsis_id)`

**Indexes:**
- `idx_game_stats_player` on `gsis_id`
- `idx_game_stats_season` on `season`
- `idx_game_stats_week` on `(season, week)`
- `idx_game_stats_team` on `team`

---

### Table: `season_stats`

Aggregated season totals. Same stat columns as `game_stats` but summed across all weeks.

**Rows:** 15,704 | **Years:** 1999-2025

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-increment primary key |
| `gsis_id` | TEXT | **FK** → `players.gsis_id` (NOT NULL) |
| `season` | INTEGER | Season year (NOT NULL) |
| `season_type` | TEXT | REG or POST (default: REG) |
| `team` | TEXT | Primary team for season |
| `games` | INTEGER | Games played |
| *(all 35 stat columns from game_stats)* | | Summed totals |
| `fantasy_points` | REAL | Season fantasy points |
| `fantasy_points_ppr` | REAL | Season PPR fantasy points |

**Constraints:**
- `UNIQUE(gsis_id, season, season_type)`
- `FOREIGN KEY (gsis_id) REFERENCES players(gsis_id)`

**Indexes:**
- `idx_season_stats_player` on `gsis_id`
- `idx_season_stats_season` on `season`

---

### Table: `draft_picks`

Historical NFL draft data.

**Rows:** 12,670 | **Years:** 2000-2024

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Draft year |
| `round` | INTEGER | Draft round |
| `pick` | INTEGER | Overall pick number |
| `team` | TEXT | Drafting team |
| `gsis_id` | TEXT | Player GSIS ID (may be NULL for older picks) |
| `pfr_id` | TEXT | Pro Football Reference ID |
| `player_name` | TEXT | Player name |
| `position` | TEXT | Position drafted |
| `college` | TEXT | College |
| `age` | REAL | Age at draft |

**Constraints:**
- `UNIQUE(season, round, pick)`

---

### Table: `combine`

NFL Scouting Combine results.

**Rows:** 8,649 | **Years:** 2000-2024

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Combine year |
| `player_name` | TEXT | Player name |
| `position` | TEXT | Position |
| `school` | TEXT | College |
| `height` | TEXT | Height (e.g., "6-2") |
| `weight` | REAL | Weight in pounds |
| `forty` | REAL | 40-yard dash time (seconds) |
| `bench` | REAL | Bench press reps (225 lbs) |
| `vertical` | REAL | Vertical jump (inches) |
| `broad_jump` | REAL | Broad jump (inches) |
| `cone` | REAL | 3-cone drill time (seconds) |
| `shuttle` | REAL | 20-yard shuttle time (seconds) |
| `pfr_id` | TEXT | Pro Football Reference ID |
| `cfb_id` | TEXT | College Football Reference ID |

---

## Supplementary Tables

These tables provide advanced analytics from specialized data sources. Each uses different ID systems - see [ID System](#id-system--relationships) for join patterns.

### Table: `snap_counts`

Weekly snap participation by phase (offense, defense, special teams).

**Rows:** 250,336 | **Years:** 2015-2024 | **ID:** PFR ID (`pfr_player_id`)

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

**Indexes:**
- `idx_snap_counts_player` on `pfr_player_id`
- `idx_snap_counts_season_week` on `(season, week)`

**Example Row:**
```
game_id: 2015_01_BAL_DEN
pfr_player_id: VasqLo20
player: Louis Vasquez
position: G
offense_snaps: 70.0, offense_pct: 1.0
defense_snaps: 0.0, defense_pct: 0.0
st_snaps: 5.0, st_pct: 0.17
```

---

### Table: `ngs_stats`

NFL Next Gen Stats advanced metrics. Contains three stat types with different columns populated.

**Rows:** 24,068 | **Years:** 2016-2024 | **ID:** GSIS ID (`player_gsis_id`)

**Stat Type Distribution:**
- `passing`: 5,328 rows
- `receiving`: 13,329 rows
- `rushing`: 5,411 rows

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

**Indexes:**
- `idx_ngs_gsis` on `player_gsis_id`
- `idx_ngs_season_week` on `(season, week)`
- `idx_ngs_type` on `stat_type`

**Example Row (Passing - Drew Brees 2016):**
```
player_display_name: Drew Brees
player_gsis_id: 00-0020531
stat_type: passing
avg_time_to_throw: 2.42
completion_percentage: 69.99
expected_completion_percentage: 64.78
completion_percentage_above_expectation: 5.20 (CPOE)
aggressiveness: 17.53
attempts: 673, completions: 471
pass_yards: 5208, pass_touchdowns: 37
```

---

### Table: `depth_charts`

Weekly depth chart positions for all teams.

**Rows:** 869,185 | **Years:** 2001-2024 | **ID:** GSIS ID (`gsis_id`)

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

**Indexes:**
- `idx_depth_gsis` on `gsis_id`
- `idx_depth_season_week` on `(season, week)`

**Example Row:**
```
season: 2001
club_code: ATL
week: 17.0
depth_team: 1 (starter)
gsis_id: 00-0000261
full_name: Ashley Ambrose
position: CB
depth_position: RCB
formation: Defense
```

---

### Table: `pfr_advanced`

Pro Football Reference advanced statistics. Contains three stat types with different columns.

**Rows:** 6,820 | **Years:** 2018-2024 | **ID:** PFR ID (`pfr_id`)

**Stat Type Distribution:**
- `pass`: 750 rows (QB passing metrics)
- `rush`: 2,471 rows (rushing metrics)
- `rec`: 3,599 rows (receiving metrics)

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

**Indexes:**
- `idx_pfr_id` on `pfr_id`
- `idx_pfr_season` on `season`

**Example Row (Passing - Ben Roethlisberger 2018):**
```
player: Ben Roethlisberger
pfr_id: RoetBe00
stat_type: pass
pass_attempts: 675
drops: 24, drop_pct: 3.6
bad_throws: 122, bad_throw_pct: 18.3
pocket_time: 2.3
times_pressured: 119, pressure_pct: 16.7
```

---

### Table: `qbr`

ESPN Total QBR (Quarterback Rating) data.

**Rows:** 1,363 | **Years:** 2006-2023 | **ID:** ESPN ID (`player_id`)

| Column | Type | Description |
|--------|------|-------------|
| `season` | INTEGER | Season year |
| `season_type` | TEXT | "Regular" or "Postseason" |
| `game_week` | TEXT | Week or "Season Total" |
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
| `qualified` | INTEGER | 1 if qualified for rankings |

**Indexes:**
- `idx_qbr_player` on `player_id`
- `idx_qbr_season` on `season`

**Example Row (Peyton Manning 2006):**
```
player_id: 1428
name_display: Peyton Manning
season: 2006
qbr_total: 86.4
pts_added: 85.5
qb_plays: 624
epa_total: 108.8
rank: 1.0
```

---

## Play-by-Play Database (`pbp.db`)

Separate database containing every NFL play from 1999-2024. Too large (535 MB) to combine with the main database.

### Table: `play_by_play`

**Rows:** 1,230,857 | **Years:** 1999-2024 | **Columns:** 84 (trimmed from 372)

All player ID columns use **GSIS ID** format (`00-0035228`) for direct joins to `nflverse_custom.db.players.gsis_id`.

#### Game/Play Identification (7 columns)

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | TEXT | Game ID (e.g., `2024_01_KC_BAL`) |
| `play_id` | REAL | Play sequence number |
| `old_game_id` | TEXT | Legacy game ID format |
| `season` | INTEGER | Season year |
| `week` | INTEGER | Week number |
| `season_type` | TEXT | REG or POST |
| `game_date` | TEXT | Game date |

#### Timing & Situation (8 columns)

| Column | Type | Description |
|--------|------|-------------|
| `game_half` | TEXT | Half1, Half2, Overtime |
| `quarter_seconds_remaining` | REAL | Seconds left in quarter |
| `half_seconds_remaining` | REAL | Seconds left in half |
| `game_seconds_remaining` | REAL | Seconds left in game |
| `qtr` | REAL | Quarter (1-5) |
| `down` | REAL | Down (1-4) |
| `ydstogo` | REAL | Yards to first down/goal |
| `yardline_100` | REAL | Yards from opponent's end zone |

#### Teams & Score (7 columns)

| Column | Type | Description |
|--------|------|-------------|
| `posteam` | TEXT | Possession team |
| `defteam` | TEXT | Defense team |
| `posteam_score` | REAL | Possession team score |
| `defteam_score` | REAL | Defense team score |
| `score_differential` | REAL | Score difference |
| `home_team` | TEXT | Home team |
| `away_team` | TEXT | Away team |

#### Play Description & Type (3 columns)

| Column | Type | Description |
|--------|------|-------------|
| `desc` | TEXT | Full play description |
| `play_type` | TEXT | pass, run, punt, field_goal, kickoff, etc. |
| `yards_gained` | REAL | Net yards on play |

#### Passing Details (4 columns)

| Column | Type | Description |
|--------|------|-------------|
| `air_yards` | REAL | Depth of target |
| `yards_after_catch` | REAL | YAC |
| `complete_pass` | REAL | 1 if completed |
| `incomplete_pass` | REAL | 1 if incomplete |

#### Play Outcome Flags (8 columns)

| Column | Type | Description |
|--------|------|-------------|
| `first_down` | REAL | 1 if first down |
| `rush` | REAL | 1 if rush play |
| `pass` | REAL | 1 if pass play |
| `sack` | REAL | 1 if sack |
| `touchdown` | REAL | 1 if TD |
| `interception` | REAL | 1 if INT |
| `fumble` | REAL | 1 if fumble |
| `fumble_lost` | REAL | 1 if fumble lost |

#### Player IDs (20 columns - all GSIS ID format)

| Column | Type | Description |
|--------|------|-------------|
| `passer_player_id` | TEXT | Passer GSIS ID |
| `passer_player_name` | TEXT | Passer name |
| `rusher_player_id` | TEXT | Rusher GSIS ID |
| `rusher_player_name` | TEXT | Rusher name |
| `receiver_player_id` | TEXT | Receiver GSIS ID |
| `receiver_player_name` | TEXT | Receiver name |
| `fantasy_player_id` | TEXT | Primary fantasy player GSIS ID |
| `fantasy_player_name` | TEXT | Primary fantasy player name |
| `kicker_player_id` | TEXT | Kicker GSIS ID |
| `kicker_player_name` | TEXT | Kicker name |
| `punter_player_id` | TEXT | Punter GSIS ID |
| `punter_player_name` | TEXT | Punter name |
| `interception_player_id` | TEXT | INT defender GSIS ID |
| `interception_player_name` | TEXT | INT defender name |
| `fumbled_1_player_id` | TEXT | Fumbler GSIS ID |
| `fumbled_1_player_name` | TEXT | Fumbler name |
| `solo_tackle_1_player_id` | TEXT | Tackler GSIS ID |
| `solo_tackle_1_player_name` | TEXT | Tackler name |
| `sack_player_id` | TEXT | Sacker GSIS ID |
| `sack_player_name` | TEXT | Sacker name |

#### Situational Flags (8 columns)

| Column | Type | Description |
|--------|------|-------------|
| `fantasy` | TEXT | Fantasy-relevant position |
| `shotgun` | REAL | 1 if shotgun formation |
| `no_huddle` | REAL | 1 if no huddle |
| `qb_dropback` | REAL | 1 if QB dropback |
| `qb_scramble` | REAL | 1 if QB scramble |
| `qb_spike` | REAL | 1 if spike |
| `pass_location` | TEXT | left, middle, right |
| `run_location` | TEXT | left, middle, right |
| `run_gap` | TEXT | end, tackle, guard |

#### Field Goals & Extra Points (7 columns)

| Column | Type | Description |
|--------|------|-------------|
| `field_goal_attempt` | REAL | 1 if FG attempt |
| `field_goal_result` | TEXT | made, missed, blocked |
| `kick_distance` | REAL | FG distance |
| `extra_point_attempt` | REAL | 1 if XP attempt |
| `extra_point_result` | TEXT | good, failed, blocked |
| `two_point_attempt` | REAL | 1 if 2PT attempt |
| `two_point_conv_result` | TEXT | success, failure |

#### Penalties (3 columns)

| Column | Type | Description |
|--------|------|-------------|
| `penalty` | REAL | 1 if penalty |
| `penalty_yards` | REAL | Penalty yardage |
| `penalty_team` | TEXT | Team penalized |

#### Advanced Metrics (5 columns)

| Column | Type | Description |
|--------|------|-------------|
| `epa` | REAL | Expected Points Added |
| `wp` | REAL | Win Probability |
| `wpa` | REAL | Win Probability Added |
| `cp` | REAL | Completion Probability |
| `cpoe` | REAL | Completion % Over Expected |

#### Drive Info (3 columns)

| Column | Type | Description |
|--------|------|-------------|
| `drive` | REAL | Drive number |
| `fixed_drive` | REAL | Corrected drive number |
| `drive_play_count` | REAL | Play number in drive |

**Indexes:**
- `idx_pbp_game` on `game_id`
- `idx_pbp_season_week` on `(season, week)`
- `idx_pbp_passer` on `passer_player_id`
- `idx_pbp_rusher` on `rusher_player_id`
- `idx_pbp_receiver` on `receiver_player_id`
- `idx_pbp_fantasy` on `fantasy_player_id`
- `idx_pbp_play_type` on `play_type`

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
JOIN players p ON s.gsis_id = p.gsis_id
WHERE p.display_name = 'Patrick Mahomes'
ORDER BY s.season;
```

### Find RBs with passing touchdowns (trick plays)

```sql
SELECT
    p.display_name,
    g.season,
    g.week,
    g.opponent,
    g.passing_yards,
    g.passing_tds
FROM game_stats g
JOIN players p ON g.gsis_id = p.gsis_id
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
JOIN players p ON s.gsis_id = p.gsis_id
WHERE s.season_type = 'REG'
ORDER BY s.fantasy_points DESC
LIMIT 10;
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
    p.current_team,
    q.qbr_total,
    q.pts_added,
    q.qb_plays
FROM qbr q
JOIN player_ids pi ON q.player_id = pi.espn_id
JOIN players p ON pi.gsis_id = p.gsis_id
WHERE q.season = 2023
  AND q.game_week = 'Season Total'
ORDER BY q.qbr_total DESC
LIMIT 10;
```

### Red zone efficiency from play-by-play

```sql
-- Attach pbp.db when needed
ATTACH DATABASE 'pbp.db' AS pbp;

SELECT
    p.passer_player_name,
    COUNT(*) as red_zone_passes,
    SUM(p.touchdown) as pass_tds,
    ROUND(100.0 * SUM(p.touchdown) / COUNT(*), 1) as td_rate
FROM pbp.play_by_play p
WHERE p.season = 2024
  AND p.yardline_100 <= 20
  AND p.pass = 1
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

**Note:** Kicker stats (FG/XP) are not tracked in nflverse player stats.

---

## Data Source

Data is sourced from [nflverse](https://github.com/nflverse) via the `nfl_data_py` Python package (archived Sept 2025; successor is `nflreadpy`).

---

## Build Scripts

```bash
# Core nflverse DB (1999-2024)
python3 build_nflverse_db.py

# Recent years only
python3 build_nflverse_db.py --start-year 2020

# Supplementary tables (snap counts, NGS, depth charts, PFR advanced, QBR)
python3 add_supplementary_tables.py

# Play-by-play database (separate)
python3 build_pbp_db.py --start-year 1999 --end-year 2024
```

---

## 2025 Data Note

nflverse has not yet released 2025 player stats (only schedule/scores). 2025 stats in this database were migrated from the old fantasyDB:
- 10,234 game stat rows (QB, RB, WR, TE) with team and opponent
- 602 season stat rows with team

The `players` table updates independently and includes 2025 rookies with headshots, bio data, and cross-platform IDs.

## Team Column Note

The `season_stats.team` column is not populated by the nflverse source data (`recent_team` is absent from seasonal imports). It is backfilled from `game_stats` using the most common team per player-season. For 2025 data, team comes from the old fantasyDB's player records. If you rebuild the database, re-run the backfill to populate this column.
