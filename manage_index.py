
import argparse
import logging
import sys

from pyramid.config import Configurator
from pyramid.paster import get_appsettings, setup_logging, bootstrap
from pyramid.scripts.common import parse_vars

from nefertari import engine
from nefertari.elasticsearch import ES
from nefertari.utils import dictset, split_strip, to_dicts

# Reminder:
# engine : is the sqla database (ie: postgres)
# ES : is the connection/wrapper object to ES

# TODO: find a way to remove a mapping in ES ! If I'm not misleading, it should remove
# all the associate documents. And we can keep our index.
# A better way is to re-index things but here we need to manage some alias over
# the indexes and ... nefertari is quite not ready I guess.

# TODO: With a simple threaded design like producer/consumer we can speed things
# a lot by keeping SQL request ahead by 1 or 2 request while ES bulk insert is
# running.


log = None

def available_models():
    models = engine.get_document_classes()
    return [ name for name, model in models.items()
             if getattr(model, '_index_enabled', False) ]

def recreate_index():
    global log
    log.info('Deleting index')
    ES.delete_index()
    log.info('Creating index')
    ES.create_index()
    log.info('Creating mappings')
    ES.setup_mappings()

def recreate_mapping(model):
    """ Delete and Put the mapping of a model.
    Effect: delete all document linked to this mapping in the working index.
    """
    global log
    mcls = engine.get_document_cls(model)
    es = ES(model)
    # delete: work with elasticsearch=1.7.0
    es.api.indices.delete_mapping(es.index_name, doc_type=model)
    # put good old mapping.
    es.put_mapping(body=mcls.get_es_mapping())

def parse_args():
    """Command line argument parser
    """
    parser = argparse.ArgumentParser(
                        description='XML parser testing')
    parser.add_argument('ini', type=str,
            help='project .ini file')
    parser.add_argument('-o', '--options', nargs='*', default=list(),
            help='overwrite configuration value')
    parser.add_argument('-m', '--models', nargs='*', default=list(),
            help='models to re-index')
    parser.add_argument('-l', '--list-models', dest="list", action="store_true",
            help='list available models')
    parser.add_argument('--recreate', action="store_true",
            help='Delete current index, create a new one and push mappings')
    parser.add_argument('--delete_mapping', action="store_true",
            help='Delete current index, create a new one and push mappings')
    parser.add_argument('--boxsize', default=5000, type=int,
            help='Size of each box extracted from DB, and send to ES')
    args = parser.parse_args()
    return args

def reindextask(model, boxsize=5000):
    """Index model by small chunks (ie: a box, with a reasonable size)
    """
    global log
    mcls = engine.get_document_cls(model)
    # proceed by chunks of 'boxsize'
    count = mcls.get_collection(_count=True)
    if count < 1: # Hu ? nothing in DB
        return
    # Let us be aware of some numbers
    boxes = count // boxsize
    rest  = count % boxsize
    es = ES(source=model) # humm quick & dirty: get a connector
    log.info('Processing model `{}` with {} documents in {} boxes'.format(model,
        count, boxes))
    # dump by 'boxes' ; add one for the rest (NB: if rest=0 the last box will be
    # empty anyway )
    for n in range(boxes+1):
        log.info('Indexing missing `{}` documents (box: {}/{})'.format(model, n,
            boxes+1))
        query_set = mcls.get_collection(_limit=boxsize, _page=n,
                _sort=mcls.pk_field()) ## don't forget the sort
        documents = to_dicts(query_set)
        log.debug('---> from db {} documents ; send to ES'.format(len(documents)))
        ## TODO: add a control ? The last box size should be equal to 'rest'
        es.index(documents)
        # here I call index : index_missing is quite frightening and most RAM is
        # used to dump from DB, from ES and compare both. It work for a few rows
        # ... but not with millions. Besides no matter ES can do the comparison
        # if the submited document is already here and the PUSH don't change
        # anything the _version isn't bumped.

def main(argv=sys.argv):
    global log
    args = parse_args()
    options = parse_vars(args.options)
    settings = get_appsettings(args.ini, name="hathor", options=options)
    setup_logging(args.ini)
    log = logging.getLogger()

    ## Init underlaying FW -- Code inspire from nefertari
    # see: https://github.com/ramses-tech/nefertari/blob/master/nefertari/scripts/es.py
    mappings_setup = getattr(ES, '_mappings_setup', False)
    try:
        ES._mappings_setup = True
        env = bootstrap("%s#hathor" % args.ini)
        ## Not sure if I need this, bootstrap should take care of this
        config = Configurator(settings=settings)
        config.include('nefertari.engine')
    finally:
        ES._mappings_setup = mappings_setup

    registry = env['registry']
    ES.setup( dictset(registry.settings) )

    # Recreate: drop index, get all available model names -- ignore -m from
    # arguments list
    if args.recreate and not args.list:
        recreate_index()
        args.models = available_models()

    if args.list:
        print("Available models:\n {}".format(
            ", ".join( available_models())) )
    elif args.models:
        model_names = args.models
        if model_names:
            av_models = available_models()
            for elem in model_names:
                if not elem in av_models:
                    raise(ValueError("model '{}' not available."
                    "Use '-l' to list available models.".format(elem)))

        ## still here we gonna re-index things
        for model in model_names:
            if args.delete_mapping:
                recreate_mapping(model)
            reindextask(model, boxsize=args.boxsize)
