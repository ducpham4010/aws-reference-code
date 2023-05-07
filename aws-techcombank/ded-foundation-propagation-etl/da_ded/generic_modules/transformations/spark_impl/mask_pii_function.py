import re
import string
from datetime import datetime

import pyffx

from da_ded.generic_modules.transformations.spark_impl.utils import normalize_phone

ISDIGIT = 1
ISLOWER = 2
ISUPPER = 3
PUNCTUATION = 4
LISTMONTH31DAYS = [1, 3, 5, 7, 8, 10, 12]
LISTMONTH30DAYS = [4, 6, 9, 11]


def cal_number_of_days(y, m):
    leap = 0
    if y % 400 == 0:
        leap = 1
    elif y % 100 == 0:
        leap = 0
    elif y % 4 == 0:
        leap = 1
    if m == 2:
        return 28 + leap
    if m in LISTMONTH31DAYS:
        return 31
    return 30


def create_mask_word_function(key, mask_f: callable):
    def mask_value(word):
        try:
            if word is None:
                return None
            word = str(word)
            word = word.strip()
            return mask_f(key, word)
        except Exception as e:
            raise e

    return mask_value


def create_mask_plain_text_function(key, mask_f: callable):
    def mask_value(plain_text):
        try:
            if plain_text is None:
                return None
            splitted_text = plain_text.split()
            results = []
            for word in splitted_text:
                word_masked = mask_f(key, word)
                results.append(word_masked)
            results = " ".join(i for i in results)
            return results
        except Exception as e:
            raise e

    return mask_value


def fpe(key: str, plain_text: str, alphabet: str) -> str:
    key = key.encode()
    try:
        e = pyffx.String(key, alphabet=alphabet, length=len(plain_text))
        return e.encrypt(plain_text)
    except:
        return plain_text


def seq_fpe(key: str, plain_text: list, alphabet: list) -> list:
    key = key.encode()
    try:
        e = pyffx.Sequence(key, alphabet=alphabet, length=len(plain_text))
        return e.encrypt(plain_text)
    except Exception:
        return plain_text


def get_character_type(character: str) -> int:
    if character.isdigit():
        return ISDIGIT
    if character.islower():
        return ISLOWER
    if character.isupper():
        return ISUPPER
    return PUNCTUATION


def mask_sequence(key: str, sequence: str) -> str:
    sequence_type = get_character_type(sequence[0])
    if sequence_type == ISDIGIT:
        return fpe(key, sequence, string.digits)
    if sequence_type == ISLOWER:
        return fpe(key, sequence, string.ascii_lowercase)
    if sequence_type == ISUPPER:
        return fpe(key, sequence, string.ascii_uppercase)
    return sequence


def mask_word(key: str, word: str) -> str:
    return mask_standard_string(key, word)


def mask_email(key: str, word: str) -> str:
    try:
        if word is None:
            return None
        regex_rule = r"[^@]+[^@]+"
        masked_email = fpe_with_regex_keep_last(word, key, regex_rule)
        return masked_email
    except Exception as e:
        return word


def fpe_with_regex_keep_last(plain_text: str, key: str, regex_rule: str):
    rule_matcher = re.match(regex_rule, plain_text)
    masked_phone_number = plain_text
    if rule_matcher is not None:
        start_char, stop_char = rule_matcher.span()
        masked_phone_number = fpe_with_matcher_keep_last(plain_text, key, start_char, stop_char)
    return masked_phone_number


def fpe_with_regex_keep_first(plain_text: str, key: str, regex_rule: str):
    rule_matcher = re.match(regex_rule, plain_text)
    masked_phone_number = plain_text
    if rule_matcher is not None:
        start_char, stop_char = rule_matcher.span()
        masked_phone_number = fpe_with_matcher_keep_first(plain_text, key, start_char, stop_char)
    return masked_phone_number


def fpe_with_matcher_keep_first(plain_text: str, key: str, start_char: int, stop_char: int):
    return plain_text[start_char: stop_char] + mask_standard_string(key, plain_text[stop_char:])


def fpe_with_matcher_keep_last(plain_text: str, key: str, start_char: int, stop_char: int):
    return mask_standard_string(key, plain_text[start_char: stop_char]) + plain_text[stop_char:]


def mask_phone_number(key: str, phone_number: str) -> str:
    phone_number = normalize_phone(phone_number)
    masked_phone_number = phone_number
    phone_len = len(phone_number)
    try:
        if phone_number.startswith("("):
            no_open_parentheses = phone_number.count("(")
            if phone_number.startswith("(+") and no_open_parentheses == 1:
                regex_rule = r"[(+]{2}[0-9]{2}[)]{1}(|\s|)\b[0-9]{2}"
                masked_phone_number = fpe_with_regex_keep_first(phone_number, key, regex_rule)
            elif no_open_parentheses == 1:
                regex_rule = r"[(]{1}([0-9]{2,3})[)]{1}[|\s|]"
                masked_phone_number = fpe_with_regex_keep_first(phone_number, key, regex_rule)
            elif no_open_parentheses == 2:
                regex_rule = r"[(+]{2}[0-9]{2}[)]{1}(|\s|)[(]{1}[0-9]{2,3}[)]{1}"
                masked_phone_number = fpe_with_regex_keep_first(phone_number, key, regex_rule)
        elif phone_number.startswith("+"):
            regex_rule = r"[+]{1}[0-9]{2}(|\s|)[0-9]{2}"
            masked_phone_number = fpe_with_regex_keep_first(phone_number, key, regex_rule)
        else:
            if phone_len == 10:
                masked_phone_number = phone_number[0:3] + mask_standard_string(key, phone_number[3:])
            elif phone_len == 11:
                masked_phone_number = phone_number[0:4] + mask_standard_string(key, phone_number[4:])
        return masked_phone_number
    except Exception as e:
        return masked_phone_number


def create_mask_datetime_function(format: str):
    def mask_date_time(key: str, datetime_str: datetime) -> str:
        try:
            datetime_parsed = datetime.strptime(datetime_str, format)
        except ValueError as e:
            return datetime_str
        month_parsed = datetime_parsed.month
        if month_parsed == 2:
            masked_month = seq_fpe(key, [month_parsed], list(range(1, 13)))[0]
        elif month_parsed in LISTMONTH31DAYS:
            masked_month = seq_fpe(key, [month_parsed], LISTMONTH31DAYS)[0]
        else:
            masked_month = seq_fpe(key, [month_parsed], LISTMONTH30DAYS)[0]
        number_of_days = cal_number_of_days(datetime_parsed.year, datetime_parsed.month)
        masked_day = seq_fpe(key, [datetime_parsed.day], list(range(1, number_of_days + 1)))[0]
        masked_datetime = datetime_parsed.replace(day=masked_day, month=masked_month)
        return masked_datetime.strftime(format)

    return mask_date_time


def mask_standard_string(key: str, word: str) -> str:
    try:
        if word is None:
            return None
        start_char = word[0]
        start_char_type = get_character_type(start_char)
        result = []
        for i in range(1, len(word)):
            char_i_type = get_character_type(word[i])
            if char_i_type == start_char_type:
                start_char += word[i]
            else:
                masked_value = mask_sequence(key, start_char)
                result.append(masked_value)
                start_char = word[i]
                start_char_type = char_i_type
        result.append(mask_sequence(key, start_char))
        result = "".join(i for i in result)
        return result
    except:
        return word