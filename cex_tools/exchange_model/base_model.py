# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : base_model
@Description :
@Time        : 2023/12/6 15:34
"""


class TradeDirection:
    short = "SELL"
    long = "BUY"


class BaseModel:

    def to_json(self):
        ret = {}
        for k, v in self.__dict__.items():
            if isinstance(v, (int, str)):
                ret[k] = v
            elif isinstance(v, list):
                ret[k] = [item.to_json() for item in v]
            elif isinstance(v, BaseModel):
                ret[k] = v.to_json()
            elif isinstance(v, dict):
                ret[k] = {k1: v1.to_json() if isinstance(v1, BaseModel) else v1 for k1, v1 in v.items()}
            else:
                ret[k] = v
        return ret

    def from_json(self, json_info):
        for k, v in json_info.items():
            if isinstance(v, (int, str)):
                self.__setattr__(k, v)
            elif isinstance(v, list):
                self.__setattr__(k, [BaseModel().from_json(item) for item in v])
            elif isinstance(v, dict):
                self.__setattr__(k, {k1: BaseModel().from_json(v1) if isinstance(v1, dict) else v1 for k1, v1 in
                                     v.items()})
            else:
                self.__setattr__(k, v)
        return self

    def __str__(self):
        infos = list(filter(lambda x: x is not None,
                            map(lambda x: '%s: %s' % (x[0], x[1]) if x[1] is not None else None, vars(self).items())))
        return '\n%s(%s)' % (
            type(self).__name__,
            '\n'.join(infos)
        )

    def __repr__(self):
        return self.__str__()


class OkxBaseModel(BaseModel):

    def __init__(self, _pair):
        self._pair = _pair

    @property
    def pair(self):
        if self._pair is None:
            raise ValueError("Pair is not set")
        return self._pair.replace("-SWAP", "").replace("-", "")


class HyperLiquidBaseModel(BaseModel):

    def __init__(self, _pair):
        self._pair = _pair

    @property
    def pair(self):
        if self._pair is None:
            raise ValueError("Pair is not set")
        return self._pair.replace("USDT", "")
