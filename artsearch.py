import sys
from glob import glob
import cPickle as pickle

from trwbl import Document, Field, Index, Tokenizer
import artsy

def create_index():
    index = Index(fields=(
        Field('title', weight=0.8, copy_to='title_str'),
        Field('title_str', store=False, weight=0.9, tokenizer=None),
        Field('date', index=False),
        Field('keywords', weight=0.7, copy_to='keywords_str'),
        Field('keywords_str', tokenizer=None),
        Field('description', weight=0.6),
        Field('contents', store=False),
    ))
    for article in glob('../art/*'):
        print "Adding %s ..." % article,
        file_handle = open(article)
        data = file_handle.read()
        file_handle.close()
        fields = artsy.get_file_dict(data)
        doc = Document(
            title=fields['title'],
            date=fields['date'],
            keywords=[x.strip() for x in fields['keywords'].split(',')],
            description=fields['description'],
            contents=fields['contents'],
        )
        index.add(doc)
        print "done."
    index.save('index')
    
def search_index(query):
    index = Index('index')
    documents = index.search(query)
    for document in documents:
        print "%s %s" % (document['title'], document['keywords'])

def get_tokens(field):
    index = Index('index')
    field = index.fields[field]
    return field.get_token_list()

if __name__ == '__main__':
    if len(sys.argv) > 2:
        if sys.argv[1] == 'search':
            print "Searching index ..."
            search_index(sys.argv[2])
        elif sys.argv[1] == 'tokens':
            print "Tokens for %s:" % sys.argv[2]
            tokens = get_tokens(sys.argv[2])
            for token_value, token_locations in tokens:
                print "\t %s (%s locations)" % (token_value, 
                        len(token_locations))
    else:
        print "Creating index ..."
        create_index()
