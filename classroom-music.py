import csv, sqlite3, yaml
from datetime import datetime as dt
from datetime import timedelta
from time import strftime, strptime
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
                    for Dt, f in SQLtypes.items():
                        try:
                            vout = f(row[k])
                            break
                        except ValueError:
                            pass
                    dtypes[k] = Dt
                    create_command += f"{k}  {Dt}, "
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
    
    def getDefaultScript(self):
        with open(self.script, 'r') as f:
            queryText = ''.join(f.readlines())
        return queryText

    
    def dotest(self):
        self.cursor.execute('SELECT COUNT(*) FROM calendar')
        n = self.cursor.fetchone()
        print(f'found {n[0]} rows in calendar')
        with open(self.script, 'r') as f:
            queryText = ''.join(f.readlines())
        queryText = queryText.replace("REPDATE","9/8/2022")
        print(queryText)
        self.cursor.execute(queryText)
        rows = self.cursor.fetchall()
        for v in rows:
            print(f'OUT: {v}')
    
    def dayBells(self, date):
        queryText = self.getDefaultScript()
        queryText = queryText.replace("REPDATE",date)
        self.cursor.execute(queryText)
        rows = self.cursor.fetchall()
        # -- WARNING rows = [{ ... }] is highly dependent on smerge.txt --- #
        rows = [{'date':row[0], 'classTime':row[1], 'classDismissTime':row[2],
                 'offset':row[3],'end':bool(row[4]), 'signal':row[5], 'file':row[6],
                 'period':row[7], 'cName':row[8], 'section':row[9],'lesson':row[10]} 
                        for row in rows]
        return(rows)
    
    def bellTime(self, bell):
        bellTime = bell['classDismissTime'] if bell['end'] else bell['classTime']
        bellOffset = timedelta(minutes=-bell['offset'] if bell['end'] else bell['offset'])
        bellDate = dt.strptime(bell['date']+' '+bellTime,"%m/%d/%Y %H:%M") + bellOffset
        return dt.strftime(bellDate, "%Y/%m/%d %H:%M:%S")
    

if __name__ == "__main__":
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    s = Sched_db(YAML, cur)
    s.dotest()
    today = dt.strftime(dt.today(), "%-m/%-d/%Y")
    today="10/10/2022"
    bells = s.dayBells(today)
    for bell in bells:
        print(f"{s.bellTime(bell)} {bell['signal']} {bell['file']}  {bell['cName']} {bell['section']}")