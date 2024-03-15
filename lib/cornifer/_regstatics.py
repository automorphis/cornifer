from ._utilities import bytify_int, BYTES_PER_MB
from .errors import RegisterError


def get_new_id(reserved, rw_txn):
    for next_apri_id_num in range(int(rw_txn.get(CURR_ID_KEY)), MAX_NUM_APRI):

        next_id = bytify_int(next_apri_id_num, MAX_NUM_APRI_LEN)

        if next_id not in reserved:
            break

    else:
        raise RegisterError(f"Too many apris added to this `Register`, the limit is {MAX_NUM_APRI}.")

    rw_txn.put(CURR_ID_KEY, bytify_int(next_apri_id_num + 1, MAX_NUM_APRI_LEN))
    return next_id

def get_apri_id_key(apri_json):
    return APRI_ID_KEY_PREFIX + apri_json

def get_id_apri_key(apri_id):
    return ID_APRI_KEY_PREFIX + apri_id

def get_apri_id(apri_json, r_txn):
    return r_txn.get(get_apri_id_key(apri_json))

def get_apri_json(apri_id, r_txn):

    apri_json = r_txn.get(get_id_apri_key(apri_id), default = None)

    if apri_json is not None:
        return apri_json

    else:
        raise RegisterError(f"Missing `ApriInfo` id : {apri_id}")

def disk_blk_keys_exist(blk_key, compressed_key, r_txn):

    if blk_key is None:
        return False

    has_blk_key = r_txn.has_key(blk_key)
    has_compressed_key = r_txn.has_key(compressed_key)

    if has_blk_key == has_compressed_key:
        return has_blk_key

    else:
        raise RegisterError("Uncompressed/compressed `Block` key mismatch.")

def get_blk_key(compressed_key):
    return BLK_KEY_PREFIX + compressed_key[COMPRESSED_KEY_PREFIX_LEN:]

def get_compressed_key(blk_key):
    return COMPRESSED_KEY_PREFIX + blk_key[BLK_KEY_PREFIX:]

def join_disk_blk_data(prefix, apri_id, startn_bytes, len_bytes):
    return prefix + apri_id + KEY_SEP + startn_bytes + KEY_SEP + len_bytes

#################################
#           CONSTANTS           #
KEY_SEP                   = b"\x00\x00"
START_N_HEAD_KEY          = b"head"
START_N_TAIL_LEN_KEY   = b"tail_length"
CLS_KEY                   = b"cls"
MSG_KEY                   = b"msg"
SUB_KEY_PREFIX            = b"sub"
BLK_KEY_PREFIX            = b"blk"
APRI_ID_KEY_PREFIX        = b"apri"
ID_APRI_KEY_PREFIX        = b"id"
CURR_ID_KEY               = b"curr_id"
APOS_KEY_PREFIX           = b"apos"
COMPRESSED_KEY_PREFIX     = b"compr"
LEN_LEN_KEY               = b"lenlen"
KEY_SEP_LEN               = len(KEY_SEP)
SUB_KEY_PREFIX_LEN        = len(SUB_KEY_PREFIX)
BLK_KEY_PREFIX_LEN        = len(BLK_KEY_PREFIX)
APRI_ID_KEY_PREFIX_LEN    = len(APRI_ID_KEY_PREFIX)
ID_APRI_KEY_PREFIX_LEN    = len(ID_APRI_KEY_PREFIX)
COMPRESSED_KEY_PREFIX_LEN = len(COMPRESSED_KEY_PREFIX)
APOS_KEY_PREFIX_LEN       = len(APOS_KEY_PREFIX)
IS_NOT_COMPRESSED_VAL     = b""
SUB_VAL                   = b""

#################################
#            LMDB KEYS          #

START_N_TAIL_LEN_DFL = 12
LEN_LEN_DFL          = 7
MAX_LEN_DFL          = 10 ** LEN_LEN_DFL - 1
STARTN_HEAD_DFL      = 0
INIT_REG_SIZE_DFL    = 5 * BYTES_PER_MB
MAX_NUM_APRI_LEN     = 6
MAX_NUM_APRI         = 10 ** MAX_NUM_APRI_LEN