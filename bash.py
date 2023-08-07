import os
import subprocess
import util
from rich import print as print

current_directory = os.path.dirname(__file__)
boilerplate_shell_script_path = os.path.join(current_directory, 'boilerplate.sh')
boilerplate_shell_script: str
with open(boilerplate_shell_script_path, 'r') as boilerplate_shell_script_file:
    boilerplate_shell_script = boilerplate_shell_script_file.read()


def run_script(script_content):
    script_path = "/tmp/migrations.sh"
    with open(script_path, 'w+') as script_file:
        script_file.write(script_content)
    os.chmod(script_path, 0o777)
    subprocess.run([script_path], check=True)


def delete_migrations(file, migrations):
    global boilerplate_shell_script
    new_script = boilerplate_shell_script
    committed_file_content = file['committed_file']['content']
    new_script += f"\n{committed_file_content}\n"
    for migration in migrations:
        migration_func_name = migration['name']
        migration_rollback_name = util.get_rollback_function_name(migration_func_name)
        new_script += f"""\n{migration_rollback_name}\n"""
    run_script(new_script)


def add_migrations(file, migrations):
    global boilerplate_shell_script
    new_script = boilerplate_shell_script
    parsed_file_content = file['parsed_file']['content']
    new_script += f"\n{parsed_file_content}\n"
    for migration in migrations:
        migration_func_name = migration['name']
        new_script += f"""\n{migration_func_name}\n"""
    run_script(new_script)
