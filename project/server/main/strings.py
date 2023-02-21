import re
from tokenizers import normalizers
from tokenizers.normalizers import BertNormalizer, Sequence, Strip
from tokenizers import pre_tokenizers
from tokenizers.pre_tokenizers import Whitespace

normalizer = Sequence([BertNormalizer(clean_text=True,
        handle_chinese_chars=True,
        strip_accents=True,
        lowercase=True), Strip()])
pre_tokenizer = pre_tokenizers.Sequence([Whitespace()])

def normalize(x, min_length = 1):
    if not isinstance(x, str):
        return ''
    normalized = normalizer.normalize_str(x)
    normalized = normalized.replace('\n', ' ')
    normalized = re.sub(' +', ' ', normalized)
    return " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > min_length])
