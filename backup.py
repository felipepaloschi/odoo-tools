"""Usage: backup.py (<dbuser> <dbpasswd>) [options]
          backup.py -h | --help

Options:
  -h --help
  -d --database DATABASE  database to execute backup
  -f --filestore FILESTORE  path to filestore
  -p --path PATH  path to store backup
"""

import os
import time
import tempfile
import subprocess
from docopt import docopt
from psycopg2 import connect
from shutil import make_archive, rmtree

def check_args(args):
    if not ["<dbuser>"] or not ["<dbpasswd>"]:
        exit(
            "(<dbuser> <dbpasswd>) are required!\
                \n Use '-h' for help"
        )

def _databases_to_execute(args):
    if args["--database"]:
        return [args["--database"]]
    connection = connect(
        dbname="postgres",
        user=args["<dbuser>"],
        host="localhost",
        password=args["<dbpasswd>"],
    )
    cursor = connection.cursor()
    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
    databases = cursor.fetchall()
    return [a[0] for a in databases]

def exec_pg_environ(**kwargs):
    env = os.environ.copy()
    env['PGHOST'] = 'localhost'
    env['PGPORT'] = '5432'
    env['PGUSER'] = kwargs['<dbuser>']
    env['PGPASSWORD'] = kwargs['<dbpasswd>']
    return env

def exec_pg_command(name, *args, **kwargs):
    env = exec_pg_environ(**kwargs)
    command = [name] + list(args)
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Command `{name}` failed with error: {result.stderr}")
    return result.stdout

def run_backup(args):
    databases = _databases_to_execute(args)
    backup_path = args.get('--path')

    if not backup_path:
        raise ValueError("The '--path' argument is required.")

    # Ensure the backup path exists
    os.makedirs(backup_path, exist_ok=True)

    for database in databases:
        if database == "postgres":
            continue
        try:
            dump_dir = tempfile.mkdtemp()
            cmd = [database, "--no-owner"]
            dump_path = os.path.join(dump_dir, "dump.sql")
            cmd.insert(-1, "--file=" + dump_path)
            exec_pg_command("pg_dump", *cmd, **args)

            # Create the zip file directly in the specified path
            name_to_store = "%s_%s.zip" % (database, time.strftime("%d_%m_%Y"))
            zip_path = os.path.join(backup_path, name_to_store)
            make_archive(zip_path.replace('.zip', ''), 'zip', dump_dir)

            name_store = "%s_%s_filestore.zip" % (database, time.strftime("%d_%m_%Y"))

            base_dir = args["--filestore"] or "/opt/dados/filestore/"

            home = os.path.join(base_dir, database)
            if not os.path.exists(home):
                continue

            filestore_zip_path = os.path.join(backup_path, name_store)
            make_archive(filestore_zip_path.replace('.zip', ''), 'zip', home)

        finally:
            rmtree(dump_dir)


if __name__ == "__main__":
    args = docopt(__doc__)
    check_args(args)

    run_backup(args)
