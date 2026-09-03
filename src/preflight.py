"""Small non-mutating Snowflake connectivity check for local troubleshooting."""

from .snowflake_loader import connection_from_environment


def main() -> None:
    with connection_from_environment() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()"
            )
            account, database, schema, warehouse, role = cur.fetchone()
    print(f"Connected: account={account}; database={database}; schema={schema}; warehouse={warehouse}; role={role}")


if __name__ == "__main__":
    main()
