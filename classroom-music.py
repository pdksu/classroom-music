# -*- coding: future_fstrings -*-
import csv, sqlite3, yaml
from time import strptime
from typing import OrderedDict
from pathlib import Path

YAML = "classroom-music.yaml"

def ctime(text : str):
    return strptime(text, "%H:%M")

def cdate(text : str):
    return strptime(text, "%m:%D:%Y")

def def_ok(x):
    return True

SQLtypes = OrderedDict({'INTEGER' : int, 'REAL' : float, 'DATE' : cdate, 'TIME' : ctime, 'TEXT' : def_ok})

def csv_to_sql(fname : Path, cur : sqlite3.Cursor, table : str ):
    ''' read_csv
    arguments: 
        fname : Path to .csv file
        con : connection object
        table : table name
    returns: 
        n : number of rows read
    '''

    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table};'")
    texists = cur.fetchone()
    n = 0
    with open(fname, "r") as f:
        r = csv.DictReader(f)
        for row in r:
            if not texists:
                create_command = f"CREATE TABLE {table} ("
                dtypes = {}
                for k in r.fieldnames:
                    for dt,f in SQLtypes.items():
                        try:
                            vout = f(row[k])
                            break
                        except ValueError:
                            pass
                    dtypes[k] = dt
                    create_command += f"{k}  {dt}, "
                create_command = create_command[0:-2] + ");"
                cur.execute(create_command)
                db_result = cur.fetchall()
                texists = True
            break

    with open(fname, "r") as f:
        r = csv.DictReader(f)
        data_out = [(row) for row in r]

    for d in data_out:
        insertCommand = f"""INSERT INTO {table} ({', '.join(r.fieldnames)}) VALUES ("{'","'.join([d[v] for v in r.fieldnames])}");"""
        cur.executescript(insertCommand)

    return len(data_out)
        

class Sched_db:
    def __init__(self, f_yaml : str, cur : sqlite3.Cursor):
        self.cursor = cur
        y = yaml.safe_load_all(Path(f_yaml).read_text()).__next__()
        self.dir = y['directory']
        self.files = y['objects']
        self.script = y['merge']
        for t, f in self.files.items():
            n=csv_to_sql(Path(self.dir,f), cur, t)
            print(f'{t} has {n} rows')
    
    def dotest(self):
        self.cursor.execute('SELECT COUNT(*) FROM calendar')
        n = self.cursor.fetchone()
        print(f'found {n[0]} rows in calendar')
        with open(self.script, 'r') as f:
            queryText = ''.join(f.readlines())
        print(queryText)
        self.cursor.execute(queryText)
        rows = self.cursor.fetchall()
        for v in rows:
            print(f'OUT: {v}')


if __name__ == "__main__":
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    s = Sched_db(YAML, cur)
    s.dotest()