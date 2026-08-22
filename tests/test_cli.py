from cassandra_trader.cli import main


def test_main(capsys) -> None:
    main()

    captured = capsys.readouterr()

    assert captured.out == "Cassandra Trader\n"