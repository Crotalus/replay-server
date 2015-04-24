
from contextlib import contextmanager

@contextmanager
def keepref(of_object, in_container):
    try:
        if isinstance(in_container, list):
            in_container.append(of_object)
        elif isinstance(in_container, set):
            in_container.add(of_object)

        yield
    finally:
        in_container.remove(of_object)