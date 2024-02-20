from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source='https://github.com/magdielgutierrez/productos_don_joaquin.git',
        entrypoint="flows_read_inventary.py:flows_read_inventory",
    ).deploy(
        name="productos_don_joaquin_",
        work_pool_name="productos_don_joaquin_produccion",
        cron="0 13 * * *",
    )