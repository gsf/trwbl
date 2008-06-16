"""
This is the trwbl library, for indexing and searching small numbers
of small documents.

Example
-------
>>> doc = Document(title='The Troll Mountain',
... author='Eleanor McGearyson',
... keywords='Troll')
>>> len(d.fields)
3
>>> for field in doc:
...     if field.name == 'author':
...          print '%s: %s' % (field.name, field.value)
...
author: Eleanor McGearyson
>>> doc2 = Document(title='Hoopdie McGee',
... author='Horrible Masterson',
... keywords=('baby', 'soup'))
>>> doc2.tokenize()
>>> doc2.token_fields['keywords']['soup'][0].field
1
>>> for field_name in doc2.token_fields:
...     for token_value in doc2.token_fields[field_name]:
...          if token_value == 'soup':
...              print field_name
...
keywords
>>> indie = Index()
>>> indie.add(doc)
>>> indie.add(doc2)
>>> indie_dump = indie.dump()
>>> new_indie = Index()
>>> new_indie.load(indie_dump)
>>> doc_list = new_indie.search('baby', field='keywords')
>>> for doc in doc_list:
...     for field in doc:
...         if field.name == 'title':
...             print field.value
...
Hoopdie McGee
"""

import pickle
import re

class Field(object):
    """
    >>> field = Field()
    Traceback (most recent call last):
      File "<stdin>", line 1, in ?
    TypeError: __init__() takes at least 3 arguments (1 given)
    >>> field = Field('title', 'The Stranger')
    >>> print field.name
    title
    """
    def __init__(self, name, value, index=True, store=True, tokenize=True):
        self.name = name
        # value can be a string or list
        # no, on second thought, value is always a string (unicode?)
        # I'd rather multiple fields than lists as values
        self.value = value
        self.store = store
        self.tokenize = tokenize
        self.index = index

class Token(object):
    """
    The smallest being in trwbl's reality, akin to a "word".  
    >>> t = Token('bob', 0, 0)
    >>> print t
    bob
    """
    def __init__(self, value, position, field):
        self.value = value
        # position is position within the field
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

class IndexedField(object):
    """
    An index of values for a given field. 
    """
    def __init__(self, name):
        self.name = name
        # self.values will be {value: [position0, position1, ...]}
        self.values = {}

    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        for value in self.values:
            yield value

    def __str__(self):
        return self.name

class Document(object):
    """
    Fields are stored as a dictionary of lists.

    >>> d = Document(title='Born Sober', 
    ...         author=['Jake Mahoney', 'Sewell Littletrout'])
    >>> d['title']
    ['Born Sober']
    >>> d['author']
    ['Jake Mahoney', 'Sewell Littletrout']
    """
    def __init__(self, **kwargs):
        #self.field_dict = {}
        self.fields = []
        self.indexed_fields = {}
        self.add(**kwargs)

    def __iter__(self):
        for field in self.fields:
            yield field

    def __getitem__(self, key):
        value_list = []
        for ref in self.field_dict[key]:
            value_list.append(self.fields[ref].value)
        return value_list

    def add(self, **kwargs):
        self.fields.append(Field(key, value))
        # handle all of this in indexed_fields
        #def field_append(key, value):
        #    self.fields.append(Field(key, value))
        #    # store reference to field list position in self.field_dict
        #    # XXX: i think there's a slicker way to do this
        #    # with python dicts
        #    if key in self.field_dict:
        #        self.field_dict[key].append(len(self.fields)-1)
        #    else:
        #        self.field_dict[key] = [len(self.fields)-1]
        #for key in kwargs:
        #    if isinstance(kwargs[key], (tuple, list)):
        #        for value in kwargs[key]:
        #            field_append(key, value)
        #    else:
        #        field_append(key, kwargs[key])

    def index(self):
        "Index the fields in the document."
        # tokenizing particulars should be extracted into Tokenizer class
        tokens_re = re.compile(r'[^.,\s]+')
        #tokens = []
        #values = []
        for field_position, field in enumerate(self.fields):
            # below needs to be fixed because as it stands
            # the same token from multiple values assigned to the same
            # field could have the exact same document & position
            # -- either tokens from multiple value assignments to the 
            # same field need to be appended before tokenizing or 
            # tokens need a record of distinct assignments
            # XXX: answer -- added field attribute to Token
            if field.index:
                try:
                    indexed_field = self.indexed_fields[field.name]
                except KeyError:
                    indexed_field = IndexedField(field.name)
                if field.tokenize:
                    token_values = tokens_re.findall(field.value)
                    for position, value in enumerate(token_values):
                        value = value.lower()
                        token = Token(value, position, field_position)
                if indexed_field.get(value):
                    indexed_field[value].append(indexed)
                else:
                    indexed_field[value] = [indexed]
                self.indexed_fields[field.name] = indexed_field

        # wait to implement frequency
#                    values.append(t.value)
#        for position in xrange(len(values)):
#            count = values.count(values[position])
#            frequency = float(count) / len(values)
#            tokens[position].frequency = frequency
        #return token_fields

class Collection(object):
    """
    Simply a list of documents. 
    """
    pass

class Index(object):
    """
    Very nearly the most important class in trwbl.
    """
    def __init__(self, filename=None):
        if filename:
            self.open(filename)
        else:
            self.documents = []
            self.fields = {}
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

    def open(self, filename):
        index_handle = open(filename, 'rb')
        try:
            self.load(pickle.load(index_handle))
        finally:
            index_handle.close()
    
    def output(self):
        'Outputs the index as a structure of lists and dictionaries.'
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

    def save(self, filename):
        index_handle = open(filename, 'wb')
        try:
            pickle.dump(self.dump(), index_handle, -1)
        finally:
            index_handle.close()

    def search(self, query, field='text'):
        query_re = re.compile(r'')
        documents = []
        if self.token_fields[field].tokens.get(query):
            for token in self.token_fields[field].tokens[query]:
                hit = self.documents[token.document]
                if hit not in documents:
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

