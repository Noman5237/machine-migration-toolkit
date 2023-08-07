import os
from os.path import expanduser
import re
import hashlib
from rich import print as print
import yaml

home = expanduser("~")
default_migrations_dir = f"{home}/.migrations"


def find_migration_projects(migrations_dir=default_migrations_dir):
    project_paths = []

    for root, dirs, files in os.walk(migrations_dir):
        if 'project.yaml' in files:
            project_paths.append(root)

    return project_paths


def list_migrations_files(project_dir):
    files = [f for f in os.listdir(project_dir) if
             os.path.isfile(os.path.join(project_dir, f)) and not f == 'project.yaml']
    files.sort()  # Sort the list alphabetically
    return files


def get_migrations_by_file_path(file_path):
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            file_hash = calculate_sha256(content)
            parsed_file = {
                "content": content,
                "hash": file_hash
            }
            parsed_migrations = parse_migrations_from_file(content)
            return parsed_file, parsed_migrations
    except FileNotFoundError:
        return None, []


def calculate_sha256(input_string):
    sha256 = hashlib.sha256()
    sha256.update(input_string.encode('utf-8'))
    return sha256.hexdigest()


def get_rollback_function_name(function_name):
    prefix_parts = function_name.split('_')[:2]
    rollback_function_name = '_'.join(prefix_parts).replace('cs', 'rb')
    return rollback_function_name


def parse_migrations_from_file(file_content):
    function_pattern = re.compile(r'(\w+)\s*\(\)\s*\{([\s\S]*?)^}', re.MULTILINE)
    function_hashes = {}

    for match in function_pattern.finditer(file_content):
        full_match = match.group(0)
        function_name = match.group(1)
        function_hashes[function_name] = calculate_sha256(full_match)

    migrations = []
    for function_name, function_hash in function_hashes.items():
        if function_name.startswith('rb'):
            continue
        rollback_function_name = get_rollback_function_name(function_name)
        if rollback_function_name not in function_hashes:
            print(
                f"[yellow]Warning! migration '{function_name}' is skipped because rollback function not found[/yellow]")
            continue
        rollback_hash = function_hashes[rollback_function_name]
        migrations.append({
            "name": function_name,
            "hash": function_hash,
            "rollback_hash": rollback_hash
        })

    return migrations


def get_project_info(project_dir):
    project_config_path = f"{project_dir}/project.yaml"
    with open(project_config_path, 'r') as file:
        content = file.read()
        return yaml.safe_load(content)
