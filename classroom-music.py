import argparse
from crontab import CronTab
import csv, sqlite3, yaml
from datetime import datetime as dt
from datetime import timedelta
from time import strftime, strptime
from typing import OrderedDict
from pathlib import Path

YAML = "classroom-music.yaml"

def csv_to_sql(fname : Path, cur : sqlite3.Cursor, table : str ):
    ''' csv_to_sql
    arguments: 
        fname : Path to .csv file
        con : connection object
        table : table name
    returns: 
        n : number of rows read
    '''
    #   local helper functions
    def ctime(text : str):
        ''' throw error if text is not a time '''
        return strptime(text, "%H:%M")

    def cdate(text : str):
        ''' throw error if text is not a date'''
        return strptime(text, "%m:%D:%Y")

    def def_ok(x):
        return True

    SQLtypes = OrderedDict({'INTEGER' : int, 'REAL' : float, 'DATE' : cdate, 'TIME' : ctime, 'TEXT' : def_ok})

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
        y = yaml.safe_load_all(Path(Path(__file__).parent.resolve(),f_yaml).read_text()).__next__()
        self.dir = Path(Path(__file__).parent.resolve(),y['directory'])
        self.music_dir = y['music']
        self.files = y['objects']
        self.script = y['merge']
        for t, f in self.files.items():
            n=csv_to_sql(Path(self.dir,f), cur, t)
            print(f'{t} has {n} rows')
    
    def list(self):
        print(f'{__file__}:\nSchedules are in {self.dir}')
        q1 = "SELECT DISTINCT teacher FROM teachers;"
        self.cursor.execute(q1)
        results = self.cursor.fetchall()
        for (i,result) in enumerate(results):
            print(f'Teacher {i+1}: {result[0]}')
        q2 = "SELECT DISTINCT schedule FROM bells;"
        self.cursor.execute(q2)
        results = self.cursor.fetchall()
        for (i,result) in enumerate(results):
            print(f'Bell Schedule {i+1}: {result[0]}')

    def getDefaultScript(self):
        with open(Path(Path(__file__).parent.resolve(),self.script), 'r') as f:
            queryText = ''.join(f.readlines())
        return queryText

    def dotest(self):
        self.cursor.execute('SELECT COUNT(*) FROM calendar')
        n = self.cursor.fetchone()
        print(f'found {n[0]} rows in calendar')
        with open(Path(Path(__file__).parent.resolve(),self.script), 'r') as f:
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
                 'offset':row[3],'end':bool(row[4]), 'signal':row[5],
                 'file': Path(self.music_dir,row[6]),
                 'period':row[7], 'cName':row[8], 'section':row[9],'lesson':row[10]} 
                        for row in rows]
        return(rows)

    def bellTime(self, bell):
        bellTime = bell['classDismissTime'] if bell['end'] else bell['classTime']
        bellOffset = timedelta(minutes=(-1 if bell['end'] else 1) * bell['offset'])
        print(f"BELL DEBUG {bell['date']}, {bellTime}, {bellOffset} ===")
        try:
            bellDate = dt.strptime(bell['date']+' '+bellTime,"%m/%d/%Y %H:%M") + bellOffset
        except TypeError:
            return None
        return bellDate


class CronScheduler:
    def __init__(self, yaml : str):
        y = yaml.safe_load_all(Path(Path(__file__).parent.resolve(),yaml).read_text()).__next__()
        self.AMRUNTIME = (y['runtime']['hour'], y['runtime']['minute'])
        self.CRONUSER = y['user']

    def schedule_bell(self, bell, testonly=False):
        command = f"cvlc --play-and-exit {bell['file']}"
        if not bell['datetime']:
            return
        print(f"BELL SCHEDULE: {bell['datetime']} {command}")
        with CronTab(user=self.CRONUSER) as cron:  # cvlc --random --play-and-exit /path/to/your/playlist
                job = cron.new(command=command)
                job.setall(bell['datetime'])
                if not testonly:
                    cron.write()

    def empty_cron(self):
        with CronTab(user=self.CRONUSER) as cron:
            vlcJobs = cron.find_command("vlc")
            for job in vlcJobs:
                cron.remove(job)
            cron.write()

    def playDate(self, date : str, dB : Sched_db, testonly=False):
        self.empty_cron()
        for bell in dB.dayBells(date):
            bell['datetime'] = dB.bellTime(bell)
            if bell['datetime']:
                self.schedule_bell(bell, testonly=testonly)

    def show_cron(self):
        with CronTab(user=self.CRONUSER) as cron:
            for job in cron:
                print(job)

    def initialize_me(self):
        with CronTab(user=self.CRONUSER) as cron:
            PYTHON = Path(Path(__file__).parent.resolve(),"venv/bin/python")
            command = f"{PYTHON} {__file__}"
            job = cron.new(command=command)
            job.setall(f'{self.AMRUNTIME[1]} {self.AMRUNTIME[0]} * * *')
            cron.write()

def getargs(args=None):
    parser = argparse.ArgumentParser(description="CRON music scheduler for school")
    parser.add_argument('-i','--initialize',help='install program in CRON for daily updates', default=False, action='store_true')
    parser.add_argument('-o','--overide',help='use a date other than today format "m/d/Y"', default=None)
    parser.add_argument('-b','--bellschedule',help='override schedule on calendar', default=None)
    parser.add_argument('-l','--list',help='show location of data and list available schedules and teachers', default=False, action='store_true')
    parser.add_argument('-t','--test',help='run and print new bells, but do not schedule', default=False, action='store_true')
    parser.add_argument('-c','--cronList',help='show list of existing cron jobs', default=False, action='store_true')
    parser.add_argument('-y','--yamlfile',help=f'specify controlling YAML file ({YAML})', default=YAML)
    args = parser.parse_args(args=args)
    return(args)


def run(args=getargs(), testonly=False):
    sched_db = sqlite3.connect(":memory:")
    sched_db_cursor = sched_db.cursor()
    sched_builder = Sched_db(args.yamlfile, sched_db_cursor)
    scheduler = CronScheduler(args.yamlfile)

    if args.initialize:
        scheduler.initialize_me()
        return
    print(f'ARGS: {args}')
    if args.list:
        sched_builder.list()
        return
    if args.overide:
        today=args.overide
    else:
        today = dt.strftime(dt.today(), "%-m/%-d/%Y")
    bells = sched_builder.dayBells(today)
    if args.bellschedule:
        def newsignal(sd : dict, sig : str):
            sd['signal'] = sig
            return(sd)
        bells = [newsignal(bell, args.bellschedule) for bell in bells]
    if bells:
        scheduler.empty_cron()
        for bell in bells:
            bell['datetime'] = sched_builder.bellTime(bell)
            scheduler.schedule_bell(bell, testonly=args.test)
    if args.cronList:
        scheduler.show_cron()

if __name__ == "__main__":
    run()