# dbt AI Agent — Project Context

## Database
- **Engine:** PostgreSQL (local)
- **Host:** localhost:5432
- **User:** arthursherman
- **Database:** tpch
- **Connection string:** postgresql://arthursherman@localhost:5432/tpch
- **Schema:** public
- **Dataset:** TPC-H scale factor 0.1

## Tables
region, nation, customer, supplier, part, partsupp, orders, lineitem

## dbt Profile
Target profile name: `lastfm_agent` (to be renamed `tpch_agent`)
profiles.yml location: ~/.dbt/profiles.yml

## Python Environment
- venv located at: ./venv
- Activate: source venv/bin/activate

## Planning
Full project plan is in PLANNING.md
