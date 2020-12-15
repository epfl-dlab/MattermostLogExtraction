import re
from collections import Counter
from collections import defaultdict

def get_liwc_groups(path):
    """
    Parses LIWC file
    return:
    - {word with suffixed endings : [LIWC groups]}
    - {LIWC group : set([complete words])}
    - [liwc group names]
    """
    liwc = open(path, 'r')
    star_words = defaultdict(list) # prefix : categories
    all_words = defaultdict(set) # category : words 
    liwc_names = [] 
    for line in liwc:
        l = line.split()
        g = l[0]
        liwc_names.append(g)
        words = l[1:]
        for w in words:
            w = w.lower()
            if w.endswith('*'):
                w = w[:-1]
                star_words[w].append(g)
            else:
                all_words[g].add(w) 
    liwc.close()
    return star_words, all_words, liwc_names
    
def get_liwc_features(text, star_words, all_words, liwc_names):
    text = text.lower()
    text = re.sub("[^\w\d'\s]+", '', text)
    tok_counts = Counter(text.split())
    new_features = [0]*len(liwc_names)
    indices = {k: v for v, k in enumerate(liwc_names)}
    for g in all_words:
        new_features[indices[g]] += sum([c for x,c in tok_counts.items() if x in all_words[g]])
    for tok in tok_counts:
        for w in star_words:
            if tok.startswith(w): 
                groups = star_words[w]
                for g in groups:
                    new_features[indices[g]] += tok_counts[tok]
    return new_features