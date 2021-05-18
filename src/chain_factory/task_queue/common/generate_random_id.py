import uuid


def generate_random_id() -> str:
    """
    Generate a unique id to be used as a unique workflow id
    """
    return str(uuid.uuid1()) + '-' + str(uuid.uuid4())
