import sys
from glob import glob

from trwbl import Document, Field, Index 
import artsy

def create_index():
    index = Index(fields=(
        Field('title', weight=0.8),
        Field('date', index=False),
        Field('keyword', weight=0.7, copy_to='keyword_s'),
        Field('keyword_s', weight=0, tokenizer=None),
        Field('description', weight=0.6),
        Field('content', store=False),
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
            keyword=[x.strip() for x in fields['keywords'].split(',')],
            description=fields['description'],
            content=fields['contents'],
        )
        index.add(doc)
        print "done."
    print len(index.documents)
    index.save('index')
    
def search_index(query):
    index = Index('index')
    results = index.search(query)
    for document in results.documents:
        print "%s %s" % (document.id, document['title'])

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
