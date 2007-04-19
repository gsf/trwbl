"""
This is the "trwbl" library, for indexing and searching small numbers
of documents.

Example
-------
>>> d = Document()
>>> d.add(Field('title', 'The Troll Mountain'))
>>> d.add(Field('author', 'Eleanor McGearyson'))
>>> d.add(Field('keywords', 'Troll'))
>>> d.fields[0].name
'title'
>>> len(d.fields)
3
>>> for field in d:
...     print '%s: %s' % (field, field.value)
...
title: The Troll Mountain
author: Eleanor McGearyson
keywords: Troll
>>> d2 = Document()
>>> d2.add(Field('title', 'Hoopdie McGee'))
>>> d2.add(Field('author', 'Horrible Masterson'))
>>> d2.add(Field('keywords', 'baby'))
>>> d2.add(Field('keywords', 'soup'))
>>> d2.tokenize()
>>> for field_name in d2.token_fields:
...     for token_value in d2.token_fields[field_name]:
...          if token_value == 'soup':
...              print field_name
...
keywords
>>> f = 'indie_test'
>>> indie = Index(f)
>>> indie.add(d)
>>> indie.add(d2)
>>> indie.save()
>>> new_indie = Index(f)
>>> new_indie.documents[0].fields[0].name
'title'
>>> new_indie.documents[1].fields[1].value
'Horrible Masterson'
>>> doc_list = new_indie.search('baby')
>>> doc_list[0].fields[0].value
'Hoopdie McGee'
"""
import os
import cPickle

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
    The smallest unit in trwbl's reality, akin to a "word".  
    Really quite close to the center of trwbl's functionality.
    """
    def __init__(self, value, position):
        self.value = value
        self.position = position
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

#    def __getitem__(self, key):
#        return self.tokens[key]

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
    def __init__(self):
        self.fields = []

    def __iter__(self):
        for field in self.fields:
            yield field

    def add(self, field):
        self.fields.append(field)

    # TODO: tokens need to be collected at the index object -- 
    # but keep the "tokenize" method on Document -- Index will
    # tokenize each document and add to Index's token_fields 
    # upon each Index.add(Document)
    #
    # for tokenized fields across the whole index:
    # field name -- token value -- doc, position (frequency to be added)
    #                           -- doc, position
    #                           -- doc, position
    #            -- token value -- doc, position
    #                           -- doc, position
    # field name -- token value -- doc, position
    #            -- token value -- doc, position
    #                           -- doc, position
    def tokenize(self):
        self.token_fields = {}
        #tokens = []
        #values = []
        for field in self.fields:
            # TODO: replace split() with an re findall()
            # that drops punctuation and lowercases --
            # also, below needs to be fixed because as it stands
            # the same token from multiple assignments to the same
            # field could have the exact same document & position
            # -- either words from multiple assignments to the same
            # field need to be appended before tokenizing or 
            # tokens need a record of distinct assignments
            if field.tokenize == True:
                try:
                    tf = self.token_fields[field.name]
                except KeyError:
                    tf = TokenField(field.name)
                words = field.value.split()
                for position in xrange(len(words)):
                    value = words[position].lower()
                    t = Token(value, position) 
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

class Index(object):
    """
    Puts all the documents in a pickle.
    Very nearly the most important class in trwbl.
    """
    def __init__(self, pickle_filename):
        self.pickle_filename = pickle_filename
        if os.path.exists(pickle_filename):
            pickle_file = open(pickle_filename, 'rb')
            self.documents = cPickle.load(pickle_file)
            self.token_fields = cPickle.load(pickle_file)
            pickle_file.close()
        else:
            self.documents = []
            self.token_fields = {}

    def add(self, document):
        document.tokenize()
        # TODO: strip unstored fields from document
        self.documents.append(document)
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

    def save(self):
        pickle_file = open(self.pickle_filename, 'wb')
        cPickle.dump(self.documents, pickle_file, -1)
        cPickle.dump(self.token_fields, pickle_file, -1)
        pickle_file.close()

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
