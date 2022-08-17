import csv, sqlite3, yaml
from pathlib import Path

YAML = "classroom-music.yaml"

class Sched_db:
    def __init__(self, f_yaml : str):
        y = yaml.safe_load_all(Path(f_yaml).read_text()).__next__()
        self.dir = y['directory']
        self.files = y['objects']
        self.teachers = csv.reader(Path(self.dir, self.files['teachers']))
        print(self.teachers)


if __name__ == "__main__":
    s = Sched_db(YAML)