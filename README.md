# PostgreSQL Scripts

Scripts for the PostgreSQL and TimescaleBD.

### delete_from_timescaledb_compressed.py

Script to delete values from all compressed TimescaleDB chunks. Can take up to half of an hour.  
Uncompress each chunk, deletes the values and then compress chuck back again.
