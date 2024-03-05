import uuid
import pytest


def _create_unique_branch_name():
    return f"branch-{uuid.uuid4()}"


def test_diff_two_same_branches(spark, setup_repo):
    _, repo = setup_repo
    main = repo.branch("main")

    # Create a new branch, check that the tables are the same
    dev = repo.branch(_create_unique_branch_name()).create(main)
    df_main = spark.read.table(f"lakefs.`{repo.id}`.`{main.id}`.company.workers")
    df_dev = spark.read.table(f"lakefs.`{repo.id}`.`{dev.id}`.company.workers")
    assert df_main.schema == df_dev.schema
    assert set(df_main.collect()) == set(df_dev.collect())

    spark.sql(f"USE lakefs.`{repo.id}`.`{dev.id}`.company")
    print(spark.sql("SHOW TABLES"))


def test_delete_on_dev_and_merge(spark, setup_repo):
    _, repo = setup_repo
    main = repo.branch("main")
    b1 = repo.branch(_create_unique_branch_name()).create(main)
    b2 = repo.branch(_create_unique_branch_name()).create(b1)

    spark.sql(f"DELETE FROM lakefs.`{repo.id}`.`{b2.id}`.company.workers WHERE id = 6")
    b2.commit(message="delete one row")
    b2.merge_into(b1)
    df_source = spark.read.table(f"lakefs.`{repo.id}`.`{b1.id}`.company.workers")
    df_dest = spark.read.table(f"lakefs.`{repo.id}`.`{b2.id}`.company.workers")
    assert df_source.schema == df_dest.schema
    assert set(df_source.collect()) == set(df_dest.collect())


def test_multiple_changes_and_merge(spark, setup_repo):
    _, repo = setup_repo
    main = repo.branch("main")
    b1 = repo.branch(_create_unique_branch_name()).create(main)
    b2 = repo.branch(_create_unique_branch_name()).create(b1)

    # Perform changes in test4 branch
    spark.sql(f"DELETE FROM lakefs.`{repo.id}`.`{b2.id}`.company.workers WHERE id = 6")
    spark.sql(f"DELETE FROM lakefs.`{repo.id}`.`{b2.id}`.company.workers WHERE id = 5")
    spark.sql(f"INSERT INTO lakefs.`{repo.id}`.`{b2.id}`.company.workers VALUES (7, 'Jhon', 'Smith', 33, 'M')")
    spark.sql(f"DELETE FROM lakefs.`{repo.id}`.`{b2.id}`.company.workers WHERE id = 4")
    spark.sql(f"INSERT INTO lakefs.`{repo.id}`.`{b2.id}`.company.workers VALUES (8, 'Marta', 'Green', 31, 'F')")

    b2.commit(message="Some changes")
    b2.merge_into(b1)
    df_source = spark.read.table(f"lakefs.`{repo.id}`.`{b1.id}`.company.workers")
    df_dest = spark.read.table(f"lakefs.`{repo.id}`.`{b2.id}`.company.workers")
    assert (df_source.schema == df_dest.schema)
    assert set(df_source.collect()) == set(df_dest.collect())
