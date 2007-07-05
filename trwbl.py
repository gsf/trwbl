"""
This is the trwbl library, for indexing and searching small numbers
of small documents.

Example
-------
>>> d = Document(title='The Troll Mountain',
... author='Eleanor McGearyson',
... keywords='Troll')
>>> len(d.fields)
3
>>> for field in d:
...     if field.name == 'author':
...          print '%s: %s' % (field, field.value)
...
author: Eleanor McGearyson
>>> d2 = Document(title='Hoopdie McGee',
... author='Horrible Masterson',
... keywords=('baby', 'soup'))
>>> d2.tokenize()
>>> d2.token_fields['keywords']['soup'][0].field
1
>>> for field_name in d2.token_fields:
...     for token_value in d2.token_fields[field_name]:
...          if token_value == 'soup':
...              print field_name
...
keywords
>>> indie = Index()
>>> indie.add(d)
>>> indie.add(d2)
>>> indie_dump = indie.dump()
>>> new_indie = Index()
>>> new_indie.load(indie_dump)
>>> doc_list = new_indie.search('baby')
>>> for doc in doc_list:
...     for field in doc:
...         if field.name == 'title':
...             print field.value
...
Hoopdie McGee
"""

class Field(object):
    """
    The second very most important class in trwbl.
    """
    def __init__(self, name, value, store=True, tokenize=True):
        self.name = name
        self.value = value
        self.store = store
        self.tokenize = tokenize

    def __str__(self):
        return self.name

class Token(object):
    """
    The smallest being in trwbl's reality, akin to a "word".  
    Really quite close to the center of trwbl's functionality.
    """
    def __init__(self, value, position, field):
        self.value = value
        self.position = position
        # field points at field position in the document --
        # differentiates between different fields of the same name
        self.field = field
        # document is a placeholder -- won't know document
        # position until added to index
        self.document = None
        # worry about frequency later
        #self.frequency = None

    def __str__(self):
        return self.value

class TokenField(object):
    """
    A container full of tokens for a given field.  Surprisingly 
    necessary for the success of trwbl.
    """
    def __init__(self, name):
        self.name = name
        # self.tokens will be a {token_value: [token0, token1]} dict
        self.tokens = {}

    def __getitem__(self, key):
        return self.tokens[key]

    def __iter__(self):
        for token in self.tokens:
            yield token

    def __str__(self):
        return self.name

#    def get(self, key):
#        try:
#            return self.tokens[key]
#        except KeyError:
#            return ''

class Document(object):
    """
    The most very important class in trwbl.
    """
    def __init__(self, **kwargs):
        self.fields = []
        self.add(**kwargs)

    def __iter__(self):
        for field in self.fields:
            yield field

    def add(self, **kwargs):
        for key in kwargs:
            if type(kwargs[key]) == type(tuple()) or \
                    type(kwargs[key]) == type(list()):
                for value in kwargs[key]:
                    self.fields.append(Field(key, value))
            else:
                self.fields.append(Field(key, kwargs[key]))

    def tokenize(self):
        self.token_fields = {}
        #tokens = []
        #values = []
        for field_position in xrange(len(self.fields)):
            # TODO: replace split() with an re findall()
            # that drops punctuation and lowercases --

            # also, below needs to be fixed because as it stands
            # the same token from multiple values assigned to the same
            # field could have the exact same document & position
            # -- either words from multiple value assignments to the 
            # same field need to be appended before tokenizing or 
            # tokens need a record of distinct assignments
            # XXX: answer -- added field attribute
            field = self.fields[field_position]
            if field.tokenize == True:
                try:
                    tf = self.token_fields[field.name]
                except KeyError:
                    tf = TokenField(field.name)
                words = field.value.split()
                for token_position in xrange(len(words)):
                    value = words[token_position].lower()
                    t = Token(value, token_position, field_position)
                    if tf.tokens.get(value):
                        tf.tokens[value].append(t)
                    else:
                        tf.tokens[value] = [t]
                self.token_fields[field.name] = tf

        # wait to implement frequency
#                    values.append(t.value)
#        for position in xrange(len(values)):
#            count = values.count(values[position])
#            frequency = float(count) / len(values)
#            tokens[position].frequency = frequency
        #return token_fields

class Collection(object):
    """
    Simply a list of documents.  Useful?  trwbl thinks so.
    """
    pass

class Index(object):
    """
    Very nearly the most important class in trwbl.
    """
    def __init__(self):
        self.documents = []
        self.token_fields = {}

    def add(self, document):
        # TODO: strip unstored fields from document

        # attempting to assign document IDs
#        for field_name in document.token_fields:
#            if field_name == unique_id:
# How to set the document ID if none given?

# for tokenized fields across the index:
# field name -- token value -- doc, position (frequency to be added)
#                           -- doc, position
#                           -- doc, position
#            -- token value -- doc, position
#                           -- doc, position
# field name -- token value -- doc, position
#            -- token value -- doc, position
#                           -- doc, position

# XXX: Deleting/replacing documents is an expensive process, as far as I can
# see it, because we would need to iterate over all of the tokens in
# self.token_fields in order to remove those with that document ID. 
# Only way to avoid that is to just leave the tokens and remove only the
# document from self.documents, then put a try/except in search() to skip
# hits on missing documents.  This might necessitate an optimize() method on 
# the index.

        self.documents.append(document)
        document.tokenize()
        for field_name in document.token_fields:
            # grab document position from the document's position in
            # the index's list of documents
            # this is pretty fragile -- assign a document.id instead?
            doc_position = len(self.documents) - 1
            for token_value in document.token_fields[field_name]:
                for token in document.token_fields[field_name].tokens[token_value]:
                    token.document = doc_position
            # if a token_field of the same name already exists in the
            # index, append this field's tokens to it
            if self.token_fields.get(field_name):
                for token_value in document.token_fields[field_name]:
                    if not self.token_fields[field_name].tokens.get(token_value):
                        self.token_fields[field_name].tokens[token_value] = []
                    for token in document.token_fields[field_name].tokens[token_value]:
                        self.token_fields[field_name].tokens[token_value].append(token)
            else:
                self.token_fields[field_name] = document.token_fields[field_name]

    def dump(self):
        return self.documents, self.token_fields

    def load(self, dumped_index):
        self.documents, self.token_fields = dumped_index
    
    def output(self):
        'outputs the index as a structure of lists and dictionaries'
        documents = []
        for document in self.documents:
            document_fields = []
            for field in document.fields:
                field_as_list = [
                        {field.name: field.value},
                        field.store,
                        field.tokenize,
                    ]
                document_fields.append(field_as_list)
            documents.append(document_fields)
        token_fields = []
        full_index = []
        full_index.append(documents)
        full_index.append(token_fields)
        return full_index

    def search(self, query):
        # TODO: parse query (field_name is temporary)
        field_name = 'keywords'
        documents = []
        if self.token_fields[field_name].tokens.get(query):
            for token in self.token_fields[field_name].tokens[query]:
                hit = self.documents[token.document]
                documents.append(hit)
        return documents

def _test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    _test()

#        self.pickle_filename = pickle_filename
#        if os.path.exists(pickle_filename):
#            pickle_file = open(pickle_filename, 'rb')
#            self.documents = cPickle.load(pickle_file)
#            self.token_fields = cPickle.load(pickle_file)
#            pickle_file.close()
#        else:
#
#    def save(self):
#        pickle_file = open(self.pickle_filename, 'wb')
#        cPickle.dump(self.documents, pickle_file, -1)
#        cPickle.dump(self.token_fields, pickle_file, -1)
#        pickle_file.close()

