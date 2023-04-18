def make_text_chunks(args):
    tokenizer, text, max_len, index = args
    ids = tokenizer(text)["input_ids"]
    chunks = {"message_index": [], "text": []}
    for i in range(1 + len(ids) // max_len):
        id_chunk = ids[max_len * i : max_len * (i + 1)]
        chunks["message_index"].append(index)
        chunks["text"].append(tokenizer.decode(id_chunk))
    return chunks
