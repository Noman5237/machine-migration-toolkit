from rich.tree import Tree
from rich.progress import Progress
from rich import print as print
import bash
import db


def detect_changes(parsed_file, parsed_migrations, committed_file, committed_migrations):
    migrations = {
        "new": [],
        "deleted": [],
        "changed": [],
        "unchanged": []
    }
    if not parsed_file:
        migrations["deleted"] = committed_migrations
        return migrations
    if not committed_file:
        migrations["new"] = parsed_migrations
        return migrations

    parsed_migration_map = {migration["name"]: migration for migration in parsed_migrations}
    committed_migration_map = {migration["name"]: migration for migration in committed_migrations}

    parsed_migration_names = set(parsed_migration_map.keys())
    committed_migration_names = set(committed_migration_map.keys())

    common = parsed_migration_names.intersection(committed_migration_names)
    new = parsed_migration_names.difference(committed_migration_names)
    deleted = committed_migration_names.difference(parsed_migration_names)

    for migration_name in common:
        parsed_migration = parsed_migration_map[migration_name]
        committed_migration = committed_migration_map[migration_name]
        if parsed_migration["hash"] != committed_migration["hash"] \
                or parsed_migration["rollback_hash"] != committed_migration["rollback_hash"]:
            migrations["changed"].append(parsed_migration)
        elif parsed_file["hash"] == committed_file["hash"]:
            migrations["unchanged"].append(parsed_migration)

    for migration_name in new:
        migrations["new"].append(parsed_migration_map[migration_name])

    for migration_name in deleted:
        migrations["deleted"].append(committed_migration_map[migration_name])

    if len(common) == len(parsed_migration_names) and len(common) == len(committed_migration_names):
        if parsed_file["hash"] != committed_file["hash"]:
            migrations["changed"] = parsed_migrations

    return migrations


def print_changes(detected_migrations):
    tree = Tree("Migrations")
    for project_name, project in detected_migrations.items():
        project_tree = tree.add(project_name)
        for file_name, file in project.items():
            file_tree = project_tree.add(file_name)
            migration_updates = file["migration_updates"]
            for migration in migration_updates['unchanged']:
                file_tree.add(f"[blue] ✅ {migration['name']}[/blue]")
            for migration in migration_updates['deleted']:
                file_tree.add(f"[red] ➖ {migration['name']}[/red]")
            for migration in migration_updates['changed']:
                file_tree.add(f"[yellow] * {migration['name']}[/yellow]")
            for migration in migration_updates['new']:
                file_tree.add(f"[green] ➕ {migration['name']}[/green]")
    print(tree)


def apply_changes(detected_migrations, migrations_cursor, migrations_conn):
    print("Applying changes...")
    for project_name, project in detected_migrations.items():
        for file_name, file in project.items():
            migration_updates = file["migration_updates"]
            if len(migration_updates['deleted']) > 0:
                print(f"Deleting migrations from {project_name}/{file_name}")
                for migration in migration_updates['deleted']:
                    bash.delete_migrations(file, [migration])
                    db.delete_migrations(project_name, file_name, file, [migration], migrations_cursor,
                                         migrations_conn)
            if len(migration_updates['changed']) > 0:
                print(f"Updating migrations from {project_name}/{file_name}")
                bash.delete_migrations(file, migration_updates['changed'])
                bash.add_migrations(file, sorted(migration_updates['changed'], key=lambda x: x['name']))
                db.delete_migrations(project_name, file_name, file,
                                     migration_updates['changed'],
                                     migrations_cursor,
                                     migrations_conn)
                db.add_migrations(project_name, file_name, file,
                                  sorted(migration_updates['changed'], key=lambda x: x['name']),
                                  migrations_cursor,
                                  migrations_conn)
            if len(migration_updates['new']) > 0:
                print(f"Adding migrations from {project_name}/{file_name}")
                for migration in migration_updates['new']:
                    bash.add_migrations(file, [migration])
                    db.add_migrations(project_name, file_name, file, [migration], migrations_cursor,
                                      migrations_conn)
