#!/usr/bin/python

import sys, os, glob, re, time
from markdown import markdown
#from mako.template import Template
#from mako.lookup import TemplateLookup

def head_split(filestring):
    'Split the head from the contents.'
    # everything up to first blank line is the head
    head, contents = re.split('\n\s*\n', filestring, 1)
    return head, contents
    
def get_file_dict(filestring):
    'Convert source file into dictionary.'
    head, contents = head_split(filestring)

    # first line in the source file should be the title
    file_dict = {'title': head.splitlines()[0]}

    # super-awesome regex for grabbing metadata (here referred to
    # as terms and definitions)
    dtdd = re.compile(r'''
            ^\*\s*     # an asterisk at the beginning of the line, 
                       # optionally followed by whitespace
            (\w*):\s*  # term, followed by a colon and optional whitespace
            (.*?)      # the definition
            (?=^\*|\Z) # followed by an asterisk at the beginning of the
                       # next line OR the end of the string
            ''', re.M | re.S | re.X)

    for definition_pair in dtdd.findall(head):
        term, definition = definition_pair
        # remove newlines and extra spaces from definitions
        nice_definition = ' '.join(definition.split())
        file_dict[term] = nice_definition

    # last, but not least, is the contents of the source file
    file_dict['contents'] = contents

    return file_dict

def atom_date(time_tuple):
    'formats date as specified in Atom specs'
    return time.strftime('%Y-%m-%dT%H:%M:%S-05:00', time_tuple)

def file_time(filename):
    'get last modified time of a file in YYYY-MM-DD format'
    time_tuple = time.localtime(os.stat(filename).st_mtime)
    return atom_date(time_tuple)

def now_time():
    time_tuple = time.localtime()
    return atom_date(time_tuple)

def set_date(filename, filestring):
    head, contents = head_split(filestring)
    now = now_time()
    head = head + '\n* date: %s\n' % now
    new_filestring = '\n'.join([head, contents])
    f = open(filename, 'w')
    try: 
        f.write(new_filestring)
    finally:
        f.close()
    return now
    
def get_just_name(filename):
    'get file name without directory or extension'
    just_name = os.path.splitext(os.path.basename(filename))[0]
    return just_name

def del_html(site_dir, filenames):
    'Delete each html file for which there is no corresponding mdwn file.'

    # shave extension off of filenames 
    just_names = [get_just_name(filename) for filename in filenames]

    htmlFileList = glob.glob(os.path.join(site_dir, '*.html'))
    for htmlName in htmlFileList:
        if get_just_name(htmlName) not in just_names:
            print 'Removing %s...' % htmlName,
            os.remove(htmlName)
            print 'done.'

# for storing tuples of (date, filename, file_dict) for index creation
date_first_list = []

def process_files(filenames, site_dir):
    'Grab metadata out of source files and create html files'

    for filename in filenames:
        print 'Processing %s...' % filename,
        f = open(filename).read().decode('utf8')
        file_dict = get_file_dict(f)
        # add "last modified" metadata
        if 'date' not in file_dict:
            file_dict['date'] = set_date(filename, f)
        file_dict['created'] = file_dict['date']
        file_dict['modified'] = file_time(filename)
        file_dict['contents'] = markdown(file_dict['contents'])
        #print file_dict.keys()
        print 'done.'

        justName = os.path.splitext(os.path.basename(filename))[0]
        htmlName = os.path.join(site_dir, justName) + '.html'
        if os.path.exists(htmlName) and \
                os.path.getmtime(filename) < os.path.getmtime(htmlName):
            #print '%s not written: File is current' % htmlName
            pass  # only talk when you walk
        else:
            print 'Writing %s...' % htmlName,
            # get article template and generate html file with 
            # assigned variables
            art_tmpl = tmpl_lookup.get_template('art.html')
            out_stream = art_tmpl.render(
                title = file_dict.get('title', ''),
                created = file_dict.get('created', ''),
                modified = file_dict.get('modified', ''),
                description = file_dict.get('description', ''),
                keywords = file_dict.get('keywords', ''),
                contents = file_dict.get('contents', ''),
                count = len(filenames),
            )
            file_handle = open(htmlName, 'w')
            try:
                file_handle.write(out_stream)
                print 'done.'
            finally:
                file_handle.close()
            
        # collect file data, date first, for sorting for index.html
        link = get_just_name(filename)
        file_dict['link'] = link
        date_tuple = (file_dict['date'], file_dict)
        date_first_list.append(date_tuple)
        date_first_list.sort(reverse=True)
        if len(date_first_list) > 30:
            date_first_list.pop()

def index_n_feed(filenames):
    if date_first_list:
        dict_list = [dict_ for (date, dict_) in date_first_list]

        # write index.html
        index_html = os.path.join(site_dir, 'index.html')
        print 'Writing %s...' % index_html,
        index_tmpl = tmpl_lookup.get_template('index.html')
        #index_tmpl = Template(filename='templates/index.html')
        out_stream = index_tmpl.render(
            arts = dict_list, 
            count = len(filenames),
        )
        i = open(index_html, 'w')
        try:
            i.write(out_stream)
            print 'done.'
        finally:
            i.close()

        # write atom feed
        index_feed = os.path.join(site_dir, 'index_feed.xml')
        print 'Writing %s...' % index_feed,
        #index_feed_tmpl = tmpl_lookup.get_template('index_feed.xml')
        index_feed_tmpl = tmpl_lookup.get_template('index_feed.xml')
        out_stream = index_feed_tmpl.render(
            arts = dict_list, 
            count = len(filenames),
        )
        #i = codecs.open(index_feed, 'w', 'utf8')
        i = open(index_feed, 'w')
        try:
            i.write(out_stream)
            print 'done.'
        finally:
            i.close()
    else:
        print 'No articles found.'

def main(site_dir):
    'main function'
    filenames = glob.glob(os.path.join(site_dir, 'art/*.mdwn'))
    filenames.sort()

    del_html(site_dir, filenames)
    process_files(filenames, site_dir)
    index_n_feed(filenames)

if __name__ == '__main__':
    try: 
        site_dir = sys.argv[1]
    except IndexError: 
        site_dir = os.path.dirname(__file__)

    tmpl_lookup = TemplateLookup(
            directories=[site_dir + '/templates'], 
            module_directory=site_dir + '/mako_modules',
            input_encoding='utf-8', 
            output_encoding='utf-8')

    main(site_dir)
