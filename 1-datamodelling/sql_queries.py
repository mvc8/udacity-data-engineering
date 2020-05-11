# DROP TABLES


from collections import OrderedDict

#Specify schema of tables
schemas = {'songplays' : OrderedDict([('songplay_id', ['bigserial', 'PRIMARY KEY']), 
                                      ('start_time', ['timestamp', 'NOT NULL']), 
                                      ('user_id', ['int', 'NOT NULL']),
                                      ('level', ['varchar', 'NOT NULL']),
                                      ('song_id', ['varchar']),
                                      ('artist_id', ['varchar']),
                                      ('session_id', ['int', 'NOT NULL']),
                                      ('location', ['varchar', 'NOT NULL']),
                                      ('user_agent', ['varchar', 'NOT NULL'])]),
           'users' : OrderedDict([('user_id', ['int', 'PRIMARY KEY']), 
                                  ('first_name', ['varchar', 'NOT NULL']), 
                                  ('last_name', ['varchar', 'NOT NULL']), 
                                  ('gender', ['varchar', 'NOT NULL']), 
                                  ('level', ['varchar', 'NOT NULL'])]),
           'songs' : OrderedDict([('song_id', ['varchar', 'PRIMARY KEY']), 
                                  ('title', ['varchar', 'NOT NULL']), 
                                  ('artist_id', ['varchar', 'NOT NULL']), 
                                  ('year', ['int', 'NOT NULL']), 
                                  ('duration', ['int', 'NOT NULL'])]),
           'artists' : OrderedDict([('artist_id', ['varchar', 'PRIMARY KEY']), 
                                    ('name', ['varchar', 'NOT NULL']), 
                                    ('location', ['varchar', 'NOT NULL']), 
                                    ('latitude', ['int']), 
                                    ('longitude', ['int'])]),
           'time' : OrderedDict([('start_time', ['timestamp', 'PRIMARY KEY']), 
                                 ('hour', ['int', 'NOT NULL']), 
                                 ('day', ['int', 'NOT NULL']), 
                                 ('week', ['int', 'NOT NULL']), 
                                 ('month', ['int', 'NOT NULL']), 
                                 ('year', ['int', 'NOT NULL']), 
                                 ('weekday', ['int', 'NOT NULL'])]),
          }

dict_conflicts = {'songplays' : 'DO NOTHING',
                 'users' : 'DO UPDATE SET level=EXCLUDED.level',
                 'songs' : 'DO NOTHING',
                 'artists' : 'DO UPDATE SET (location, latitude, longitude) = (EXCLUDED.location, EXCLUDED.latitude, EXCLUDED.longitude)',
                 'time' : 'DO NOTHING',}


#dictionary containing schema information; this is to be passed to SQL CREATE functions
dict_cmd = {}
for tbl, schema in schemas.items():
    cmd = ''
    for key, val in schema.items():
        instruction = [key]
        instruction.extend(val)
        cmd = cmd + ' '.join(map(str, instruction)) + ', '
    dict_cmd[tbl] = cmd[:-2]



#Currently not implemented -- optional dictionary that holds row data to be uploaded into tables. This can be useful for default data uploads.
dict_data = {'songplays' : [()],
        'users' : [()],
        'songs' : [()],
        'artists' : [()],
        'time' : [()]                           
            }

#To adhere to project structure, specify default SQL commands
cmd_drop = []
cmd_create = []
cmd_insert = {}
for tbl, cmd in dict_cmd.items():
    cmd_drop.append("DROP TABLE IF EXISTS %s" %tbl)
    cmd_create.append("CREATE TABLE IF NOT EXISTS %s (%s);" % (tbl, cmd))
    
    
    
    #Deal with insert exceptions
    #print(schemas[tbl].items())
    primary_key = ''.join([x[0] for x in schemas[tbl].items() if 'PRIMARY KEY' in x[1]])
    #print(primary_key)
    n_cols = len(schemas[tbl].keys())
    #ignore primary keys that are sequential number handled by postgress
    if 'bigserial' in schemas[tbl][primary_key]:
        exception_schema = dict(dict(schemas[tbl]))
        exception_schema.pop(primary_key)
        cols = ', '.join(map(str, exception_schema.keys()))
        cmd_insert[tbl] = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {};".format(tbl, cols, ','.join(['%s']*(n_cols-1)), primary_key, dict_conflicts[tbl])
    else:
        cols = ', '.join(map(str, schemas[tbl].keys()))
        cmd_insert[tbl] = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {};".format(tbl, cols, ','.join(['%s']*n_cols), primary_key, dict_conflicts[tbl])

# FIND SONGS
song_select = ("SELECT a.song_id, a.artist_id from songs a left join artists b on a.artist_id=b.artist_id \
                where a.title = %s and b.name = %s and a.duration = %s")

# QUERY LISTS
create_table_queries = cmd_create
drop_table_queries = cmd_drop
song_table_insert = "INSERT INTO %s (%s) VALUES %s"