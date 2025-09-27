from wikimedia_yard_reaas_test.maps import hello_word_function_printer
import pytest


def test_hello_word_function_printer(capsys: pytest.CaptureFixture[str]) -> None:
    # Call the function
    hello_word_function_printer()

    # Capture stdout
    captured = capsys.readouterr()

    # Assert that the output is exactly what we expect
    assert captured.out == "hello word\n"
