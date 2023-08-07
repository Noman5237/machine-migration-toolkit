import sqlite3
import os


def get_migrations_db_cursor(db_dir):
    conn = sqlite3.connect(os.path.join(db_dir, 'migrations.db'))
    cursor = conn.cursor()
    setup_migrations_db(cursor)
    return cursor, conn


def setup_migrations_db(cursor):
    cursor.execute("""
        create table if not exists migrations
        (
            project_name   varchar(255) not null,
            file_name      varchar(255) not null,
            migration_name varchar(255) not null,

            migration_hash varchar(255) not null,
            rollback_hash  varchar(255),

            primary key (project_name, file_name, migration_name)
        );
    """)

    cursor.execute("""
        create table if not exists migration_files
        (
            project_name varchar(255) not null,
            file_name    varchar(255) not null,
            file_content varchar(255) not null,
            file_hash    varchar(255) not null,

            primary key (project_name, file_name)
        );
    """)

    cursor.execute("""
        create table if not exists migration_history
        (
            migration_timestamp varchar(255) not null,
            project_name varchar(255) not null,
            file_name    varchar(255) not null,
            migration_name varchar(255) not null,
            migration_hash varchar(255) not null,
            rollback_hash  varchar(255),
            migration_status varchar(255) not null,
            
            primary key (migration_timestamp)
        );
    """)


def list_migration_files(project, cursor):
    cursor.execute("""
        select distinct file_name
        from migrations
        where project_name = ?
        order by file_name asc;
    """, (project,))

    files = [row[0] for row in cursor.fetchall()]

    return files


def get_migrations_by_file_name(project_name, file_name, cursor) -> (object, list):
    cursor.execute("""
        select file_content, file_hash
        from migration_files
        where project_name = ? and file_name = ?;
    """, (project_name, file_name))

    row = cursor.fetchone()

    if not row:
        return None, []

    migration_file = {
        "content": row[0],
        "hash": row[1]
    }

    cursor.execute("""
        select migration_name, migration_hash, rollback_hash
        from migrations
        where project_name = ? and file_name = ?
        order by migration_name asc;
    """, (project_name, file_name))

    migrations = []
    for row in cursor.fetchall():
        migrations.append({
            "name": row[0],
            "hash": row[1],
            "rollback_hash": row[2]
        })

    return migration_file, migrations


def delete_migrations(project_name, file_name, file, migrations, migrations_cursor, migrations_conn):
    migrations_cursor.executemany("""
        delete from migrations
        where project_name = ? and file_name = ? and migration_name = ?;
    """, [(project_name, file_name, m["name"]) for m in migrations])

    migrations_conn.commit()
    if len(get_migrations_by_file_name(project_name, file_name, migrations_cursor)[1]) == 0:
        migrations_cursor.execute("""
            delete from migration_files
            where project_name = ? and file_name = ?;
        """, (project_name, file_name))
    else:
        migrations_cursor.execute("""
            update migration_files
            set file_content = ?, file_hash = ?
            where project_name = ? and file_name = ?;
        """, (file["parsed_file"]["content"], file["parsed_file"]["hash"], project_name, file_name))
    migrations_conn.commit()

    # add to migration_history
    migrations_cursor.executemany("""
        insert into migration_history (
            migration_timestamp, project_name, file_name,
            migration_name, migration_hash, rollback_hash, migration_status
        )
        values (datetime('now'), ?, ?, ?, ?, ?, ?);
    """, [(project_name, file_name, m["name"], m["hash"], m["rollback_hash"], "deleted") for m in migrations])


def add_migrations(project_name, file_name, file, migrations, migrations_cursor, migrations_conn):
    migrations_cursor.executemany("""
        insert into migrations (project_name, file_name, migration_name, migration_hash, rollback_hash)
        values (?, ?, ?, ?, ?);
    """, [(project_name, file_name, m["name"], m["hash"], m["rollback_hash"]) for m in migrations])

    if len(get_migrations_by_file_name(project_name, file_name, migrations_cursor)[1]) == 0:
        migrations_cursor.execute("""
            insert into migration_files (project_name, file_name, file_content, file_hash)
            values (?, ?, ?, ?);
        """, (project_name, file_name, file["parsed_file"]["content"], file["parsed_file"]["hash"]))
    else:
        migrations_cursor.execute("""
            update migration_files
            set file_content = ?, file_hash = ?
            where project_name = ? and file_name = ?;
        """, (file["parsed_file"]["content"], file["parsed_file"]["hash"], project_name, file_name))
    migrations_conn.commit()

    # add to migration_history
    migrations_cursor.executemany("""
        insert into migration_history (
            migration_timestamp, project_name, file_name,
            migration_name, migration_hash, rollback_hash, migration_status
        )
        values (datetime('now'), ?, ?, ?, ?, ?, ?);
    """, [(project_name, file_name, m["name"], m["hash"], m["rollback_hash"], "added") for m in migrations])

