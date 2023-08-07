import util
import db
import changes
import typer
import os


def main():
    migrations_cursor, migrations_conn = db.get_migrations_db_cursor(util.default_migrations_dir)
    migration_projects = util.find_migration_projects()

    detected_migrations = {}
    for project_dir in migration_projects:
        project_info = util.get_project_info(project_dir)
        project_name = project_info["name"]

        detected_migrations_projects = {}

        all_files = sorted(list(
            set(util.list_migrations_files(project_dir) + db.list_migration_files(project_name, migrations_cursor))))
        for file in all_files:
            parsed_file, parsed_migrations = util.get_migrations_by_file_path(os.path.join(project_dir, file))
            committed_file, committed_migrations = db.get_migrations_by_file_name(project_name, file, migrations_cursor)

            migration_updates = changes.detect_changes(parsed_file, parsed_migrations, committed_file,
                                                       committed_migrations)

            migration_updates = {
                "new": sorted(migration_updates["new"], key=lambda x: x["name"]),
                "deleted": sorted(migration_updates["deleted"], key=lambda x: x["name"], reverse=True),
                "changed": sorted(migration_updates["changed"], key=lambda x: x["name"], reverse=True),
                "unchanged": sorted(migration_updates["unchanged"], key=lambda x: x["name"]),
            }

            detected_migrations_projects[file] = {
                "parsed_file": parsed_file,
                "committed_file": committed_file,
                "migration_updates": migration_updates,
            }

        detected_migrations[project_name] = detected_migrations_projects

    changes.print_changes(detected_migrations)
    typer.confirm("Are you sure you want to apply changes?", abort=True)
    changes.apply_changes(detected_migrations, migrations_cursor, migrations_conn)
    migrations_conn.commit()


if __name__ == '__main__':
    typer.run(main)
