__all__ = ["GdriveLimitedPythonOperator"]


def __getattr__(name):
    if name == "GdriveLimitedPythonOperator":
        from .utils import GdriveLimitedPythonOperator

        return GdriveLimitedPythonOperator

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
