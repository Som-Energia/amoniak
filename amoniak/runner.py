#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

import click

from amoniak import tasks
from amoniak.utils import setup_logging, read_list_from_file
from amoniak import VERSION


@click.group()
@click.option('--log-level', default='info')
@click.option('--async/--no-async', default=True)
def amoniak(log_level, async):
    MODE = {True: 'ASYNC', False: 'SYNC'}
    log_level = log_level.upper()
    log_level = getattr(logging, log_level, 'INFO')
    logging.basicConfig(level=log_level)
    setup_logging()
    logger = logging.getLogger('amon')
    logger.info('Running amoniak version: %s' % VERSION)
    logger.info('Running in %s mode' % MODE[async])
    os.environ['RQ_ASYNC'] = str(async)

@amoniak.command()
@click.option('--tg_enabled', default=True)
def enqueue_all_amon_measures(tg_enabled):
    logger = logging.getLogger('amon')
    logger.info('Enqueuing all amon measures')
    tasks.enqueue_all_amon_measures(tg_enabled)


@amoniak.command()
@click.option('--tg_enabled', default=True)
@click.option('--contracts', default=[])
def enqueue_measures(tg_enabled, contracts):
    tg_enabled = False
    logger = logging.getLogger('amon')
    logger.info('Enqueuing measures')
    contracts_id = None
    try:
        contracts_id = read_list_from_file(contracts, str)
    except Exception, e:
            logger.info('Failed loading contracts: {e}'.format(**locals()))
            return
    tasks.enqueue_measures(tg_enabled, contracts_id)

@amoniak.command()
@click.option('--tg_enabled', default=True)
@click.option('--contracts', default=[])
def enqueue_contracts(tg_enabled, contracts):
    tg_enabled = False
    logger = logging.getLogger('amon')
    logger.info('Enqueuing updated contracts')
    contracts_id = None
    try:
        contracts_id = read_list_from_file(contracts, str)
    except Exception, e:
            logger.info('Failed loading contracts: {e}'.format(**locals()))
            return
    tasks.enqueue_contracts(tg_enabled, contracts_id)
    logger.info('Enqueuing new contracts')
    tasks.enqueue_new_contracts(tg_enabled, contracts_id)
    tasks.enqueue_remove_contracts(tg_enabled, contracts_id)


@amoniak.command()
def enqueue_cchfact():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing F5D curves')
    try:
        tasks.enqueue_cch('tg_cchfact', 'empowering_last_f5d_measure', True)
    except Exception as e:
        logger.info('Failed enqueuing F5D: %s', str(e))


@amoniak.command()
def enqueue_cchval():
    logger = logging.getLogger('amon')
    logger.info('Enqueuing P5D curves')
    try:
        tasks.enqueue_cch('tg_cchval', 'empowering_last_p5d_measure', False)
    except Exception as e:
        logger.info('Failed enqueuing P5D: %s', str(e))

if __name__ == '__main__':
    amoniak(obj={})
