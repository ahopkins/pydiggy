class ConflictingType(AttributeError):  # noqa
    def __init__(self, pname, incoming, existing):
        incoming_name = getattr(incoming, "__name__", repr(incoming))
        existing_name = getattr(existing, "__name__", repr(existing))

        msg = (
            f"You previously defined {pname} as {existing_name}. "
            f"You cannot now define it as {incoming_name}."
        )

        super().__init__(msg)


class NotStaged(AttributeError):  # noqa
    def __init__(self, uid):
        msg = (
            f"Cannot generate with unstaged reference: {uid}. "
            "Did you forget to call <node>.stage()"
        )

        super().__init__(msg)


class InvalidData(Exception):  # noqa
    def __init__(self, msg=""):
        msg = f"Data in invalid format. {msg}"

        super().__init__(msg)


class MissingAttribute(AttributeError):  # noqa
    def __init__(self, instance, attribute):
        msg = f"Your model instance {instance} does not have '{attribute}'. Perhaps you forgot to query it?"

        super().__init__(msg)
