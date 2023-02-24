def get_concepts(d):
    if 'ingénieur' in d.lower():
        return ['Engineering']
    if 'biologie' in d.lower():
        return ['Biology']
    if 'chimie' in d.lower():
        return ['Chemistry']
    if 'physique' in d.lower():
        return ['Physics']
    if 'mathématique' in d.lower():
        return ['Mathematics']
    if 'informatique' in d.lower():
        return ['Computer science']
    if 'médecine' in d.lower():
        return ['Medicine']
    if 'histoire' in d.lower():
        return ['History', 'Geography']
    if 'économie' in d.lower():
        return ['Economics']
    if 'management' in d.lower():
        return ['Business']
    if 'sociales' in d.lower():
        return ['Sociology']
    if 'langue' in d.lower():
        return ['Linguistics']
    if 'de la terre' in d.lower():
        return ['Environmental science', 'Geology']
    if 'littérature' in d.lower():
        return ['Literature']
    if 'philosophie' in d.lower():
        return ['Philosophy', 'Psychology']
    if 'arts' in d.lower():
        return ['Art']
    if 'astronomie' in d.lower():
        return ['Astronomy']
    if 'politique' in d.lower():
        return ['Political science']
    if 'droit' in d.lower():
        return ['Law']
    if 'éducation' in d.lower():
        return []
    if 'religion' in d.lower():
        return ['Religious studies']
    if 'paléontologie' in d.lower():
        return ['Paleontology']
    return []
