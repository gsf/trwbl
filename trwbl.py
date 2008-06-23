"""
This is the trwbl library, for indexing and searching small numbers
of small documents.

    index = Index(fields=(
        Field('title', copy_to='title_str'),
        Field('title_str', tokenizer=Tokenizer(re_str='.+')),
        Field('author'),
        Field('content', store=False),
    ))
    document = Document(
        title='Clearly Broken', 
        author='Sally Bizurtz', 
        content='I believe the world needs fixing.',
    )
    index.add(document)
    index.save('index')
    
    documents = index.search('clearly')

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
>>> doc2.fields['keywords']['soup'][0].field
1
>>> for field_name in doc2.fields:
...     for token_value in doc2.fields[field_name]:
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

class IndexException(Exception):
    pass

class Tokenizer(object):
    def __init__(self, lower=True, re_string=r'[\w\']+'):
        self.lower = lower
        self.tokens_re = re.compile(re_string)

    def tokenize(self, value):
        tokens = self.tokens_re.findall(value)
        if self.lower:
            tokens = [x.lower() for x in tokens]
        return tokens

class Field(object):
    """
    """
    def __init__(self, name, index=True, store=True, copy_to=None, 
                weight=0.5, tokenizer=Tokenizer()):
        self.name = name
        self.index = index
        self.store = store
        self.copy_to = copy_to
        self.weight = weight
        self.tokenizer = tokenizer
        # self.tokens will be {token_value: [Token, ...], ...}
        self.tokens = ListsDict()

    def __getitem__(self, key):
        return self.tokens[key]

    def __iter__(self):
        for token in self.tokens:
            yield token

    def __str__(self):
        return self.name

    def add(self, field_values, doc_position):
        for field_position, field_value in enumerate(field_values):
            token_values = self.tokenizer.tokenize(field_value)
            for position, value in enumerate(token_values):
                token = TokenPosition(position, field_position, doc_position)
                self.tokens[value] = token

class TokenPosition(object):
    """ """
    def __init__(self, str, field, doc):
        #self.value = value
        self.str = str
        self.field = field
        self.doc = doc

    def __str__(self):
        return self.str

class IndexFields(dict):
    def __init__(self, *args):
        self.add(*args)

    def __getitem__(self, field_name):
        try:
            return dict.__getitem__(self, field_name)
        except KeyError:
            raise IndexException, "Field '%s' not defined." % field_name
        
    def __setitem__(self, field_name, field):
        if field_name in self:
            raise IndexException, "Field '%s' was defined twice." % field_name
        else:
            dict.__setitem__(self, field_name, field)
                
    def add(self, *args):
        for field in args:
            self[field.name] = field
    
class Index(object):
    """ """
    def __init__(self, filename=None, fields=None):
        if filename:
            self.open(filename)
        else:
            self.documents = []
            self.fields = IndexFields()
            self.weighted_fields = []
            for field in fields:
                self.fields[field.name] = field
                if field.index and field.weight:
                    self.weighted_fields.append((field.weight, field.name))
                    self.weighted_fields.sort()
                    self.weighted_fields.reverse()

    def add(self, document):
        self.documents.append(document)
        doc_position = len(self.documents) - 1
        for field in document:
            index_field = self.fields[field]
            field_values = document[field]
            if index_field.index:
                index_field.add(field_values, doc_position)
            if index_field.copy_to:
                copy_field = self.fields[index_field.copy_to]
                if copy_field.index:  # really, when would it not be?
                    copy_field.add(field_values, doc_position)
            if not self.fields[field].store:
                document[field] = None

    def dump(self):
        #return self.documents, self.fields
        return self.__dict__

    def load(self, dumped_index):
        #self.documents, self.fields = dumped_index
        self.__dict__ = dumped_index

    def open(self, filename):
        index_handle = open(filename, 'rb')
        try:
            self.load(pickle.load(index_handle))
        finally:
            index_handle.close()
    
    def save(self, filename):
        index_handle = open(filename, 'wb')
        try:
            pickle.dump(self.dump(), index_handle, -1)
        finally:
            index_handle.close()

    def search(self, query):
        query_re = re.compile(r'')
        documents = []
        for weight, field_name in self.weighted_fields:
            try:
                tokens = self.fields[field_name][query]
            except KeyError:
                tokens = []
            for token in tokens:
                document = self.documents[token.doc]
                print "document: %s" % document.title
                print "field: %s" % field_name 
                print "position: %s" % token.str
                print
                if document not in documents:
                    documents.append(document)
        return documents

class Document(object):
    """
    Fields are stored as a dictionary of lists.

    >>> d = Document(title='Born Sober', 
    ...         author=['Jake Mahoney', 'Sewell Littletrout'])
    >>> d.title
    'Born Sober'
    >>> d.author # only gets the first
    'Jake Mahoney'
    >>> d['author'] # gets them all
    ['Jake Mahoney', 'Sewell Littletrout']
    """
    def __init__(self, **kwargs):
        self.fields = ListsDict()
        for key, value in kwargs.items():
            self.__setitem__(key, value)

    def __delitem__(self, field):
        del self.fields[field]

    def __getitem__(self, field):
        return self.fields[field]

    def __setitem__(self, key, value):
        if hasattr(value, '__iter__'):
            self.__dict__[key] = value[0]
        else:
            self.__dict__[key] = value
        self.fields[key] = value

    def __iter__(self):
        for field in self.fields:
            yield field

class ListsDict(dict):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.__setitem__(key, value)
            
    def __setitem__(self, key, value):
        if value:
            if hasattr(value, '__iter__'):
                value = list(value)
            else:
                value = [value]
            if key in self:
                dict.__getitem__(self, key).extend(value)
            else:
                dict.__setitem__(self, key, value)
        else: # leave a way to zero out the value for unstored fields
            dict.__setitem__(self, key, None)

def _test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    _test()

        # attempting to assign document IDs
#        for field_name in document.fields:
#            if field_name == unique_id:
# How to set the document ID if none given?

# for indexed fields across the index:
# field name -- value -- doc, position (frequency to be added)
#                     -- doc, position
#                     -- doc, position
#            -- value -- doc, position
#                     -- doc, position
# field name -- value -- doc, position
#            -- value -- doc, position
#                     -- doc, position

#        for field_name in document.fields:
#            # grab document position from the document's position in
#            # the index's list of documents
#            # this is pretty fragile -- assign a document.id instead?
#            doc_position = len(self.documents) - 1
#            for token_value in document.fields[field_name]:
#                for token in document.fields[field_name].tokens[token_value]:
#                    token.document = doc_position
#            # if a token_field of the same name already exists in the
#            # index, append this field's tokens to it
#            if self.fields.get(field_name):
#                for token_value in document.fields[field_name]:
#                    if not self.fields[field_name].tokens.get(token_value):
#                        self.fields[field_name].tokens[token_value] = []
#                    for token in document.fields[field_name].tokens[token_value]:
#                        self.fields[field_name].tokens[token_value].append(token)
#            else:
#                self.fields[field_name] = document.fields[field_name]

#    def output(self):
#        'Outputs the index as a structure of lists and dictionaries.'
#        documents = []
#        for document in self.documents:
#            document_fields = []
#            for field in document.fields:
#                field_as_list = [
#                        {field.name: field.value},
#                        field.store,
#                        field.tokenize,
#                    ]
#                document_fields.append(field_as_list)
#            documents.append(document_fields)
#        fields = []
#        full_index = []
#        full_index.append(documents)
#        full_index.append(fields)
#        return full_index

# XXX: Deleting/replacing documents is an expensive process, as far as I can
# see it, because we would need to iterate over all of the tokens in
# self.fields in order to remove those with that document ID. 
# Only way to avoid that is to just leave the tokens and remove only the
# document from self.documents, then put a try/except in search() to skip
# hits on missing documents.  This might necessitate an optimize() method on 
# the index.

#        self.pickle_filename = pickle_filename
#        if os.path.exists(pickle_filename):
#            pickle_file = open(pickle_filename, 'rb')
#            self.documents = cPickle.load(pickle_file)
#            self.fields = cPickle.load(pickle_file)
#            pickle_file.close()
#        else:
#
#    def save(self):
#        pickle_file = open(self.pickle_filename, 'wb')
#        cPickle.dump(self.documents, pickle_file, -1)
#        cPickle.dump(self.fields, pickle_file, -1)
#        pickle_file.close()

#class Collection(object):
#    """
#    A list of documents. 
#    """
#    pass

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

#    def index(self):
#        "Index the fields in the document."
#        # tokenizing particulars should be extracted into Tokenizer class
#        tokens_re = re.compile(r'[^.,\s]+')
#        #tokens = []
#        #values = []
#        for field_position, field in enumerate(self.fields):
#            # below needs to be fixed because as it stands
#            # the same token from multiple values assigned to the same
#            # field could have the exact same document & position
#            # -- either tokens from multiple value assignments to the 
#            # same field need to be appended before tokenizing or 
#            # tokens need a record of distinct assignments
#            # XXX: answer -- added field attribute to Token
#            if field.index:
#                try:
#                    indexed_field = self.indexed_fields[field.name]
#                except KeyError:
#                    indexed_field = IndexedField(field.name)
#                if field.tokenize:
#                    token_values = tokens_re.findall(field.value)
#                    for position, value in enumerate(token_values):
#                        value = value.lower()
#                        token = Token(value, position, field_position)
#                if indexed_field.get(value):
#                    indexed_field[value].append(indexed)
#                else:
#                    indexed_field[value] = [indexed]
#                self.indexed_fields[field.name] = indexed_field
#
        # wait to implement frequency
#                    values.append(t.value)
#        for position in xrange(len(values)):
#            count = values.count(values[position])
#            frequency = float(count) / len(values)
#            tokens[position].frequency = frequency
        #return fields


