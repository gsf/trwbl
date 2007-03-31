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
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.store = True
        self.tokenize = True

    def __str__(self):
        return self.name

    def __repr__(self):
        return '%s: %s' % (self.name, self.value)

class Token(object):
    """
    Really quite close to the center of trwbl's functionality.
    """
    def __init__(self, field, value, position):
        self.field = field
        self.value = value
        self.position = position
        self.document = None
        self.frequency = None

    def __repr__(self):
        return self.value

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

    def tokenize(self):
        token_fields = []
        tokens = []
        values = []
        for field in self.fields:
            # TODO: replace split() with an re findall()
            # that drops punctuation and lowercases
            words = field.value.split()
            for position in xrange(len(words)):
                t = Token(field.name, words[position], position) 
                tokens.append(t)
                values.append(t.value)
        for position in xrange(len(values)):
            count = values.count(values[position])
            frequency = float(count) / len(values)
            tokens[position].frequency = frequency
        return tokens

class Index(object):
    """
    Very nearly the most important class in trwbl.  Holds all the 
    documents in a pickle.
    """
    def __init__(self, pickle_filename):
        self.pickle_filename = pickle_filename
        if os.path.exists(pickle_filename):
            pickle_file = open(pickle_filename, 'rb')
            self.documents = cPickle.load(pickle_file)
            self.tokens = cPickle.load(pickle_file)
            pickle_file.close()
        else:
            self.documents = []
            self.tokens = []

    def add(self, document):
        self.documents.append(document)
        tokens = document.tokenize()
        for token in tokens:
            doc_position = len(self.documents) - 1
            token.document = doc_position
            self.tokens.append(token)

    def save(self):
        pickle_file = open(self.pickle_filename, 'wb')
        cPickle.dump(self.documents, pickle_file, -1)
        cPickle.dump(self.tokens, pickle_file, -1)
        pickle_file.close()

    def search(self, query):
        pass

def _test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    _test()
