import os

'''NB! Update!
To ckeck compression policy id, run:

    SELECT job_id, table_name
    FROM _timescaledb_config.bgw_policy_compress_chunks p
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = p.hypertable_id);

'''
job_id = '<placeholder>'

'''NB! Update!

values - array for LIKE 'postgresql_query'
'''
values = ['<placeholder>', '<placeholder>']

# PostgreSQL configurations
port = '<placeholder>'
user = 'postgres'
host = '<placeholder>'
db = 'postgres'

psql_command = 'psql -p %s -U %s -h %s -d %s -c ' % (port, user, host, db)

pause_job = '\"SELECT alter_job_schedule({}, next_start=>\'infinity\');\"'.format(job_id)
start_job = '\"SELECT alter_job_schedule({}, next_start=>now());\"'.format(job_id)

uncompressed_command = ''' \"
    SELECT chunk_name
    FROM timescaledb_information.compressed_chunk_stats
    WHERE compression_status LIKE 'Un%';
    \"
'''

compressed_command = ''' \"
    SELECT chunk_name
    FROM timescaledb_information.compressed_chunk_stats
    WHERE compression_status NOT LIKE 'Un%';
    \"
'''

decompress_command = '\"SELECT decompress_chunk(\'{}\');\"'
compress_command = '\"SELECT compress_chunk(\'{}\');\"'

labels_delete = '\"DELETE FROM metrics_labels WHERE metric_name LIKE \'{}\';\"'

delete_command_metr_name = '''\"
    DELETE FROM {}
    WHERE labels_id IN (
            SELECT id
            FROM <table_name>
            WHERE <placeholder> LIKE '{}'
        );
    \"
'''

os.popen(psql_command + pause_job)

uncompressed_chunks = (os.popen(psql_command + uncompressed_command)
                        .read()
                        .split()[2:-2])

compressed_chunks = (os.popen(psql_command + compressed_command)
                      .read()
                      .split()[2:-2])

initial_num = os.popen(psql_command + 'SELECT COUNT(*) FROM <table_name> ;').read().split()[2:-2]

if values != None:
    for chunk in uncompressed_chunks:
        for name in values:
            delete_cmd = delete_command_metr_name.format(chunk, name)
            print(os.popen(psql_command + delete_cmd).read())

    for chunk in compressed_chunks:
        print(os.popen(psql_command + decompress_command.format(chunk)).read())
        for name in values:
            delete_cmd = delete_command_metr_name.format(chunk, name)
            print(os.popen(psql_command + delete_cmd).read())
        print(os.popen(psql_command + compress_command.format(chunk)).read())

if label_names != None:
    for chunk in uncompressed_chunks:
        for entry in label_names:
            for name in label_names[entry]:
                delete_cmd = delete_command_label_name.format(chunk, entry, name)
                print(os.popen(psql_command + delete_cmd).read())

    for chunk in compressed_chunks:
        print(os.popen(psql_command + decompress_command.format(chunk)).read())
        for entry in label_names:
            for name in label_names[entry]:
                delete_cmd = delete_command_label_name.format(chunk, entry, name)
                print(os.popen(psql_command + delete_cmd).read())
        print(os.popen(psql_command + compress_command.format(chunk)).read())

os.popen(psql_command + start_job)


for name in metrics_names:
    print(os.popen(psql_command + labels_delete.format(name)).read())

final_num = os.popen(psql_command + 'SELECT COUNT(*) FROM <table_name> ;').read().split()[2:-2]

print('Before: %s , After: %s' % (initial_num, final_num))
