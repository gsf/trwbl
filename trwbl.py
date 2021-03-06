"""
This is the Trwbl library, for indexing and searching small numbers
of small documents.

Example
-------
>>> import trwbl
>>> index = trwbl.Index()
>>> index = Index(fields=(
...     Field('title', weight=0.9),
...     Field('author', weight=0.6),
...     Field('keyword', weight=0.5, copy_to='keyword_str'),
...     Field('keyword_str', weight=0, tokenizer=None),
...     Field('content', store=False),
... ))
>>> doc = trwbl.Document(
... title='The Troll Mountain',
... author='Eleanor McGearyson',
... keyword='Troll',
... )
>>> index.add(doc)
>>> doc2 = Document(
... title='Hoopdie McGee',
... author='Horrible Masterson',
... keyword=('baby', 'soup'),
... )
>>> index.add(doc2)
>>> index.save('index')
>>> index2 = Index('index')
>>> results = index2.search('baby')
>>> for doc in results.documents:
...     print doc['title']
...
Hoopdie McGee
"""

import cPickle as pickle
import re

try:  # use memcache if we got it
    import memcache
except ImportError:
    memcache = None

MEMCACHE_LOCATION = '127.0.0.1:11211'

class FieldException(Exception):
    pass

class IndexException(Exception):
    pass

QUERY_RE = re.compile(r"""
"(.+?)(?:"|$)     # anything surrounded by quotes (or to end of line)
|                 # or
([+-]?)           # grab an optional + or -
([\S]+):          # then non-whitespace, then a colon 
(
  ".+?(?:"|$)|    # then anything surrounded by quotes 
  \(.+?(?:\)|$)|  # or parentheses (or to end of line)
  [\S]+           # or non-whitespace strings
)|                # or
([+-]?)           # grab an optional + or -
([\S]+)           # and non-whitespace strings without colons
""", re.VERBOSE | re.UNICODE)
def parse_query(query):
    return QUERY_RE.findall(query)

FIELD_QUERY_RE = re.compile(r"""
"(.+?)(?:"|$)     # anything surrounded by quotes (or to end of line)
|                 # or
([+-]?)           # grab an optional + or -
([\S]+)           # and non-whitespace strings
""", re.VERBOSE | re.UNICODE)
def parse_field_query(field_query):
    return FIELD_QUERY_RE.findall(field_query)

class Tokenizer(object):
    def __init__(self, lower=True, re_string=r'[\w\']+'):
        self.lower = lower
        self.tokens_re = re.compile(re_string)

    def tokenize(self, value):
        tokens = self.tokens_re.findall(value)
        if self.lower:
            tokens = [x.lower() for x in tokens]
        return tokens

class TokenizerNot(object):
    """A non-tokenizing tokenizer."""
    def tokenize(self, value):
        return [value]

class TokenLocations(dict):
    """
    TokenLocations is a dictionary of dictionaries of lists.  The keys for 
    the outer dictionary are document IDs.  The keys for the inner 
    dictionaries are field IDs (for multiple fields with the same name in a 
    document).  The lists are token IDs, pointing to the position of each
    token in a field.
    """
    def add_location(self, doc_id, field_id, token_id):
        if doc_id in self:
            field_ids = self[doc_id]
            if field_id in field_ids:
                field_ids[field_id].append(token_id)
            else:
                field_ids[field_id] = [token_id]
        else:
            self[doc_id] = {field_id: [token_id]}
        
    def compare(self, previous_locations, proximity_range=1):
        """
        Returns close_locations and a list of all distances between 
        compared locations.  Each location in close_locations is within 
        the proximity_range of a previous location.
        """
        close_locations = TokenLocations()
        distances = []
        for doc_id in self:
            if doc_id in previous_locations:
                field_ids = self[doc_id]
                previous_field_ids = previous_locations[doc_id]
                for field_id in field_ids:
                    if field_id in previous_field_ids:
                        token_ids = field_ids[field_id]
                        previous_token_ids = previous_field_ids[field_id]
                        for token_id in token_ids:
                            for previous_token_id in previous_token_ids:
                                distance = token_id - previous_token_id
                                if distance <= proximity_range:
                                    close_locations.add_location(doc_id, 
                                            field_id, token_id)
                                distances.append(distance)
        return close_locations, distances

    def get_consecutive(self, previous_locations):
        """
        Returns a TokenLocations instance of locations from self that are 
        consecutive to locations in previous_locations.  Used for phrase 
        and complex word searches.
        """
        consecutive_locations = TokenLocations()
        for doc_id in self:
            if doc_id in previous_locations:
                field_ids = self[doc_id]
                previous_field_ids = previous_locations[doc_id]
                for field_id in field_ids:
                    if field_id in previous_field_ids:
                        token_ids = field_ids[field_id]
                        previous_token_ids = previous_field_ids[field_id]
                        for token_id in token_ids:
                            if token_id in previous_token_ids:
                                consecutive_locations.add_location(doc_id, 
                                        field_id, token_id)
        return consecutive_locations

    def get_distances(self, previous_locations):
        """
        Returns a list of integer distances from each location in 
        previous_locations to each location in self.
        """
        distances = []
        for doc_id in self:
            if doc_id in previous_locations:
                field_ids = self[doc_id]
                previous_field_ids = previous_locations[doc_id]
                for field_id in field_ids:
                    if field_id in previous_field_ids:
                        token_ids = field_ids[field_id]
                        previous_token_ids = previous_field_ids[field_id]
                        for token_id in token_ids:
                            for previous_token_id in previous_token_ids:
                                distances.append(token_id - previous_token_id)
        return distances

class Field(object):
    """
    For tokens across the index:
    
        field -- token -- doc_id -- field_id -- token_id
                                             -- token_id
                                 -- field_id -- token_id
              -- token -- doc_id -- field_id -- token_id
                                             -- token_id
        field -- token -- doc_id -- field_id -- token_id
              -- token -- doc_id -- field_id -- token_id
                                 -- field_id -- token_id
    
    doc_id, field_id, and token_id are integers that refer to list indices 
    for documents in the index, fields of the same name in a document, and 
    tokens in a field, respectively.
    """
    def __init__(self, name, index=True, store=True, copy_to=None, 
                weight=0.1, tokenizer=Tokenizer()):
        self.name = name
        self.index = index
        self.store = store
        self.copy_to = copy_to
        if 0 <= weight <= 1:
            self.weight = weight
        else:
            raise FieldException, \
            "Invalid weight: '%s'.  Weight must be between 0 and 1." % weight
        if tokenizer:
            self.tokenizer = tokenizer
        else:
            self.tokenizer = TokenizerNot()
        self.tokens = {}

    def __getitem__(self, key):
        return self.tokens[key]

    def __iter__(self):
        for token in self.tokens:
            yield token

    def __str__(self):
        return self.name

    def add(self, field_value, document_id):
        if hasattr(field_value, '__iter__'):
            field_values = field_value
        else:
            field_values = [field_value]
        for field_id, field_value in enumerate(field_values):
            token_values = self.tokenizer.tokenize(field_value)
            for token_id, value in enumerate(token_values):
                if value in self.tokens:
                    document_ids = self.tokens[value]
                    if document_id in document_ids:
                        field_ids = document_ids[document_id]
                        if field_id in field_ids:
                            field_ids[field_id].append(token_id)
                        else:
                            field_ids[field_id] = [token_id]
                    else:
                        document_ids[document_id] = {field_id: [token_id]}
                else:
                    self.tokens[value] = {document_id: {field_id: [token_id]}}
                        
    def get_token_list(self):
        """Get a list of tokens, sorted by popularity."""
        decorated_token_list = [(-len(self.tokens[x]), x) for x in self.tokens]
        decorated_token_list.sort()
        return [(x[1], self.tokens[x[1]]) for x in decorated_token_list]

class ResultSet(object):
    def __init__(self, index, query):
        # document_scores is a list of (score, document_id) tuples
        self.document_scores = [(0, x) for x in index.documents]
        # part_locations are the locations of the most recent query part
        self.previous_locations = {}
        self.index = index
        self.search(query)

    def search(self, query):
        query_parts = parse_query(query)
        for part in query_parts:
            phrase, field_op, field, field_query, word_op, word = part
            if phrase:
                self._phrase_search(phrase)
            if field_query:
                if field_query.startswith('('):
                    field_query = field_query.strip('()')
                field_query_parts = field_query_parse(field_query)
                for fq_part in field_query_parts:
                    self._field_search(fq_part, field, field_op)
            if word:
                self._word_search(word, word_op)
        return self.populate()

    def populate(self):
        self.documents = []
        self.document_scores.sort()
        self.document_scores.reverse()
        for score, document_id in self.document_scores:
            document = self.index.documents[document_id]
            document.score = score
            self.documents.append(document)
        return self

    def _field_search(self, field_query, field, field_op=None):
        pass

    def _phrase_search(self, phrase):
        pass

    def _word_search(self, word, word_op=None):
        negative = False
        if word_op:
            if word_op == '-':
                negative = True
            elif word_op == '+':
                pass  # could extend at some point
        word_locations = {}
        found_docs = []
        for weight, field_name in self.index.weighted_fields:
            #print field_name
            index_field = self.index.fields[field_name]
            tokens = index_field.tokenizer.tokenize(word)
            #print tokens
            document_ids = []
            locations = None
            previous_locations = None
            # for each token, only keep locations that are a distance of 1 from
            # a previous location
            for enum, token in enumerate(tokens):
                if token in index_field:
                    locations = index_field[token]
                else:
                    continue
                if previous_locations:
                    locations = locations.get_consecutive(previous_locations)
                previous_locations = locations
            word_locations = locations
            if negative:
                self.document_scores = [x for x in self.document_scores if 
                        x[1] not in locations]
            else:
                distances = self._get_distances(
                        self.previous_locations[field_name], document_ids)
                for enum, score_id in enumerate(self.document_scores):
                    score, id = score_id
                    if id in document_ids:
                        found_docs.append(id)
                        document = document_ids[id]
                        #print "{%s: %s}" % (id, document)
                        if field_name in locations:
                            locations[field_name][id] = document
                        else:
                            locations[field_name] = {id: document}
                        distances = self._get_distances(
                            self.previous_locations[field_name], 
                            {id: document})
                        weight_mod = [1]
                        for distance in distances:
                            if distance == 1:
                                distance = distance / 4.0
                            elif 1 < distance < 5:
                                distance = distance / 2.0
                            elif distance < 0:
                                distance = -distance
                            weight_mod.append(0.01 / distance)
                        modified_weight = weight * sum(weight_mod)
                        score += (1 - score) * modified_weight
                        self.document_scores[enum] = (score, id)
        if not negative:
            self.document_scores = [x for x in self.document_scores if 
                    x[1] in set(found_docs)]
        self.previous_locations = locations

class IndexFieldDict(dict):
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
    
class Index(object):
    """ """
    def __init__(self, filename=None, fields=None):
        if filename:
            self.open(filename)
        elif fields:
            self.documents = {}
            self.doc_counter = 0
            self.fields = IndexFieldDict()
            self.weighted_fields = []
            for field in fields:
                self.fields[field.name] = field
                if field.index and field.weight:
                    self.weighted_fields.append((field.weight, field.name))
                    self.weighted_fields.sort()
                    self.weighted_fields.reverse()

# TODO: add update and delete methods -- should there be a 
# Document.index method?

    def add(self, document):
        document.id = self.doc_counter
        self.doc_counter += 1
        self.documents[document.id] = document
        for field in document:
            index_field = self.fields[field]
            field_value = document[field]
            if index_field.index:
                index_field.add(field_value, document.id)
            if index_field.copy_to:
                copy_field = self.fields[index_field.copy_to]
                if copy_field.index:  # really, when would it not be?
                    copy_field.add(field_value, document.id)
            if not self.fields[field].store:
                document[field] = None  # can't delete during loop

    def dump(self):
        return pickle.dumps(self.__dict__, -1)

    def load(self, dumped_index):
        if self.__dict__:
            raise IndexException, "Attempted load on established index."
        self.__dict__ = pickle.loads(dumped_index)

    def get_mc(self):
        if memcache:
            mc = memcache.Client([MEMCACHE_LOCATION], debug=0)
        else:
            mc = {}
        return mc

    def open(self, filename):
        mc = self.get_mc()
        dumped_index = mc.get(filename)
        if dumped_index:
            self.load(dumped_index)
        else:
            index_handle = open(filename, 'rb')
            try:
                self.load(index_handle.read())
            finally:
                index_handle.close()
    
    def save(self, filename):
        dumped_index = self.dump()
        index_handle = open(filename, 'wb')
        try:
            index_handle.write(dumped_index)
        finally:
            index_handle.close()
        mc = self.get_mc()
        if mc:
            mc.set(filename, dumped_index)

    def search(self, query):
        # TODO: handle quoted search and power searches
        # TODO: score documents based on weighting, word proximity, and 
        #       frequency
        return ResultSet(self, query)

class Document(object):
    """
    A document is a dictionary of fields.  Use a list for fields with
    more than one value.

    >>> d = Document(title='Born Sober', 
    ...         author=['Jake Mahoney', 'Sewell Littletrout'])
    >>> d['title']
    'Born Sober'
    >>> d['author'] 
    ['Jake Mahoney', 'Sewell Littletrout']
    """
    def __init__(self, **kwargs):
        self.fields = kwargs
        self.id = None
        self.score = None

    def __getitem__(self, field):
        return self.fields[field]

    def __setitem__(self, key, value):
        self.fields[key] = value

    def __iter__(self):
        for field in self.fields:
            yield field

class ListsDict(dict):
    """A dictionary that stores all values as lists."""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.__setitem__(key, value)
            
    def __setitem__(self, key, value):
        if hasattr(value, '__iter__'):
            value = list(value)
        else:
            value = [value]
        if key in self:
            dict.__getitem__(self, key).extend(value)
        else:
            dict.__setitem__(self, key, value)

def _test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    _test()

# XXX: Deleting/replacing documents is an expensive process, as far as I can
# see it, because we would need to iterate over all of the tokens in
# self.fields in order to remove those with that document ID. 
# Only way to avoid that is to just leave the tokens and remove only the
# document from self.documents, then put a try/except in search() to skip
# hits on missing documents.  This might necessitate an optimize() method on 
# the index.

# for tokens across the index:
#
#     field -- token -- doc_id -- field_id -- token_id
#                                          -- token_id
#                              -- field_id -- token_id
#           -- token -- doc_id -- field_id -- token_id
#                                          -- token_id
#     field -- token -- doc_id -- field_id -- token_id
#           -- token -- doc_id -- field_id -- token_id
#                              -- field_id -- token_id
# 
# doc_id, field_id, and token_id refer to list indices for documents in the
# index, fields of the same name in a document, and tokens in a field, 
# respectively.

        # attempting to assign document IDs
#        for field_name in document.fields:
#            if field_name == unique_id:
# How to set the document ID if none given?

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


