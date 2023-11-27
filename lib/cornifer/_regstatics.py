from ._utilities import bytify_int, BYTES_PER_MB
from .errors import RegisterError


def get_new_id(reserved, rw_txn):
    for next_apri_id_num in range(int(rw_txn.get(_CURR_ID_KEY)), _MAX_NUM_APRI):

        next_id = bytify_int(next_apri_id_num, _MAX_NUM_APRI_LEN)

        if next_id not in reserved:
            break

    else:
        raise RegisterError(f"Too many apris added to this `Register`, the limit is {_MAX_NUM_APRI}.")

    rw_txn.put(_CURR_ID_KEY, bytify_int(next_apri_id_num + 1, _MAX_NUM_APRI_LEN))
    return next_id

def get_apri_id_key(apri_json):
    return _APRI_ID_KEY_PREFIX + apri_json

def get_id_apri_key(apri_id):
    return _ID_APRI_KEY_PREFIX + apri_id

def get_apri_json(apri_id, r_txn):

    apri_json = r_txn.get(get_id_apri_key(apri_id), default = None)

    if apri_json is not None:
        return apri_json

    else:
        raise RegisterError(f"Missing `ApriInfo` id : {apri_id}")

#################################
#           CONSTANTS           #
_KEY_SEP                   = b"\x00\x00"
_START_N_HEAD_KEY          = b"head"
_START_N_TAIL_LENGTH_KEY   = b"tail_length"
_CLS_KEY                   = b"cls"
_MSG_KEY                   = b"msg"
_SUB_KEY_PREFIX            = b"sub"
_BLK_KEY_PREFIX            = b"blk"
_APRI_ID_KEY_PREFIX        = b"apri"
_ID_APRI_KEY_PREFIX        = b"id"
_CURR_ID_KEY               = b"curr_id"
_APOS_KEY_PREFIX           = b"apos"
_COMPRESSED_KEY_PREFIX     = b"compr"
_LENGTH_LENGTH_KEY         = b"lenlen"
_KEY_SEP_LEN               = len(_KEY_SEP)
_SUB_KEY_PREFIX_LEN        = len(_SUB_KEY_PREFIX)
_BLK_KEY_PREFIX_LEN        = len(_BLK_KEY_PREFIX)
_APRI_ID_KEY_PREFIX_LEN    = len(_APRI_ID_KEY_PREFIX)
_ID_APRI_KEY_PREFIX_LEN    = len(_ID_APRI_KEY_PREFIX)
_COMPRESSED_KEY_PREFIX_LEN = len(_COMPRESSED_KEY_PREFIX)
_APOS_KEY_PREFIX_LEN       = len(_APOS_KEY_PREFIX)
_IS_NOT_COMPRESSED_VAL     = b""
_SUB_VAL                   = b""

#################################
#            LMDB KEYS          #

_START_N_TAIL_LENGTH_DEFAULT   = 12
_LENGTH_LENGTH_DEFAULT         = 7
_MAX_LENGTH_DEFAULT            = 10 ** _LENGTH_LENGTH_DEFAULT - 1
_START_N_HEAD_DEFAULT          = 0
_INITIAL_REGISTER_SIZE_DEFAULT = 5 * BYTES_PER_MB
_MAX_NUM_APRI_LEN              = 6
_MAX_NUM_APRI                  = 10 ** _MAX_NUM_APRI_LEN