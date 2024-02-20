from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        entrypoint="flows_read_inventory.py:flows_read_inventory",
    ).deploy(
        name="flows_read_inventary",
        work_pool_name="productos_don_joaquin_produccion",
        cron="0 13 * * *",
    )