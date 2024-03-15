from ._regstatics import get_apri_id, get_apri_json
from ._utilities import default_encoder, bsenc, bsdec
from .info import ApriInfo, _INFO_TYPE_KW, _Info


class RelApriInfo(ApriInfo):

    r_txn = None
    instances = []

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        type(self).instances.append(self)

    @classmethod
    def from_primitive_json(cls, json_):

        if json_[_INFO_TYPE_KW] != cls.__name__:
            raise ValueError

        if cls.r_txn is None:
            raise ValueError

        json_ = bsdec(get_apri_json(bsenc(json_['id']), cls.r_txn))
        return _Info.from_json(json_)

    def json_encode_default(self):

        json_str = default_encoder.encode(super().json_encode_default())
        return {'_Info_type' : type(self).__name__, 'id' : bsdec(get_apri_id(bsenc(json_str), type(self).r_txn))}

    def to_json(self, r_txn):

        type(self).set_r_txn(r_txn)
        return super().to_json()

    @classmethod
    def set_r_txn(cls, r_txn):

        if cls.r_txn != r_txn:

            cls.r_txn = r_txn

            for apri in cls.instances:
                apri._json = None

            cls.instances = []