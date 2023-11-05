class UNSET:
    def __repr__(self):
        return "UNSET"

    def __eq__(self, other):
        return other.__class__ == self.__class__


UNSET = UNSET()


class FieldNotFoundException(Exception):
    pass


def resolve_name(type, name):
    if not isinstance(name, str) or name[0] == "$":
        return name
    ret = type
    for part in name.split("."):
        try:
            ret = getattr(ret, part)
        except AttributeError:
            raise FieldNotFoundException("Field not found %s (in %s)" % (part, name))

    return ret


def classproperty(fun):
    class Descriptor(property):
        def __get__(self, instance, owner):
            return fun(owner)

    return Descriptor()
