class BaseEnum:
    @classmethod
    def items(cls):
        return [v for k, v in cls.__dict__.items() if type(v) == str and k[:2] != "__"]


class DotDict(dict):
    """dot access (key.value) to dictionary attributes: https://stackoverflow.com/a/23689767
    Use this temporarily when model system is not completed"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__