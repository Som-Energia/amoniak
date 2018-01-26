# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime,date,timedelta
import logging
import urllib2

import libsaas

from .utils import (
    setup_peek, setup_mongodb, setup_empowering_api, setup_redis,
    sorted_by_key, Popper, setup_queue
)
from .amon import AmonConverter, check_response
import pymongo
from rq.decorators import job
from raven import Client
from empowering.utils import make_local_timestamp


sentry = Client()
logger = logging.getLogger('amon')

def enqueue_all_amon_measures(tg_enabled=True, bucket=500):
    if not tg_enabled:
        return

    serials = open('serials', 'r')
    for serial in serials:
        meter_name = serial.replace('\n', '').strip()
        if not meter_name.startswith('ZIV00'):
            continue
        filters = {
            'name': meter_name,
            'type': 'day',
            'value': 'a',
            'valid': True,
            'period': 0
        }
        mongo = setup_mongodb()
        collection = mongo['tg_billing']
        measures = collection.find(filters, {'id': 1})
        measures_to_push = []
        for idx, measure in enumerate(measures):
            if idx and not idx % bucket:
                j = push_amon_measures.delay(tg_enabled, measures_to_push)
                logger.info("Job id:%s | %s/%s/%s" % (
                    j.id, meter_name, idx, bucket)
                )
                measures_to_push = []
            measures_to_push.append(measure['id'])
    mongo.connection.disconnect()

def enqueue_measures(tg_enabled=True, polisses_ids=[], bucket=500):
    # First get all the contracts that are in sync
    O = setup_peek()
    #em = setup_empowering_api()
    # TODO: Que fem amb les de baixa? les agafem igualment? només les que
    # TODO: faci menys de X que estan donades de baixa?
    search_params = [('etag', '!=', False),
                     ('state', '=', 'activa'),
                     ('cups.empowering', '=', True)]
    if isinstance(polisses_ids, list) and polisses_ids:
        search_params.append(('name', 'in', polisses_ids))
    pids = O.GiscedataPolissa.search(search_params)

    search_params = [('polissa', 'in', pids)]
    if tg_enabled:
        search_params.append(('tg_cnc_conn', '=', 1))

    cids = O.GiscedataLecturesComptador.search(search_params, context={'active_test': False})
    fields_to_read = ['name', 'empowering_last_measure']

    # Measurement source type filter
    #Name                               codi   subcodi   active
    #----------------------------------+------+---------+--------
    #Telemesura                        | 10   | 00      | t
    #Telemesura corregida              | 11   | 00      | f
    #TPL                               | 20   | 00      | t
    #TPL corregida                     | 21   | 00      | f
    #Visual                            | 30   | 00      | t
    #Visual corregida                  | 31   | 00      | f
    #Estimada                          | 40   | 00      | t
    #Estimada amb històric             | 40   | 01      | t
    #Estimada amb factor dutilització  | 40   | 02      | t
    #Autolectura                       | 50   | 00      | t
    #Telegestió                        | 60   | 00      | t
    #Sense Lectura                     | 99   | 00      | t
    origen_to_search = ['10','11','20','21','30','31','40','50','60']
    origen_ids = O.GiscedataLecturesOrigen.search([('codi','in',origen_to_search)])

    popper = Popper([])
    for comptador in O.GiscedataLecturesComptador.read(cids, fields_to_read):
        search_params = [
            ('comptador', '=', comptador['id'])
        ]
        if tg_enabled:
            tg_name = O.GiscedataLecturesComptador.build_name_tg(comptador['id'])
            search_params += [
                ('name', '=', tg_name),
                ('type', '=', 'day'),
                ('value', '=', 'a'),
                ('valid', '=', 1),
                ('period', '=',  0)]

        last_measure = comptador.get('empowering_last_measure')
        if not last_measure:
            logger.info(u"No hi ha lectures. Carregar totes")

        else:
            logger.info(u"Última lectura trobada: %s" % last_measure)
            if tg_enabled:
                search_params.append(('date_end', '>', last_measure))
            else:
                search_params.append(('write_date', '>', last_measure))

        if tg_enabled:
            measures_ids = O.TgBilling.search(search_params, limit=0, order="date_end asc")
        else:
            search_params.append(('origen_id', 'in', origen_ids))

            #origen_comer_to_search = [
            #    'Fitxer de lectures Q1',
            #    'Fitxer de factura F1',
            #    'Entrada manual',
            #    'Oficina Virtual',
            #    'Autolectura',
            #    'Estimada',
            #    'Gestió ATR'
            #]
            #origen_comer_ids = O.GiscedataLecturesOrigenComer.search([('name','in',origen_comer_to_search)])
            #search_params.append(('origen_comer_id', 'in', origen_comer_ids))

            measures_ids = O.GiscedataLecturesLectura.search(search_params, limit=0, order="name asc")
        logger.info("S'han trobat %s mesures per pujar" % len(measures_ids))
        popper.push(measures_ids)
    pops = popper.pop(bucket)
    while pops:
        j = push_amon_measures.delay(tg_enabled, pops)
        logger.info("Job id:%s | %s/%s" % (
             j.id, len(pops), len(popper.items))
        )
        pops = popper.pop(bucket)


def enqueue_new_contracts(tg_enabled, polisses_ids =[], bucket=500):
    search_params = [
        ('polissa.etag', '=', False),
        ('polissa.cups.empowering', '=', True)
    ]
    if tg_enabled:
        search_params.append(('tg_cnc_conn', '=', 1))
    residential_tariffs = [
        '2.0A', '2.0DHA', '2.0DHS', '2.1A', '2.1DHA', '2.1DHS'
    ]
    search_params.append(('polissa.tarifa.name', 'in', residential_tariffs))

    #em = setup_empowering_api()

    # NOTE: Remove online update, use deferred one
    #items = em.contracts().get(sort="[('_updated', -1)]")['_items']
    #if items:
    #    from_date = make_local_timestamp(items[0]['_updated'])
    #    search_params.append(('polissa.create_date', '>', from_date))

    # Enable to force 180 days backwards check instead of online checking
    from_date = (date.today() - timedelta(days=180)).strftime('%Y-%m-%d')
    search_params.append(('polissa.create_date', '<', from_date))
    if isinstance(polisses_ids, list) and polisses_ids:
        search_params.append(('polissa.name', 'in', polisses_ids))

    O = setup_peek()
    cids = O.GiscedataLecturesComptador.search(search_params,
        context={'active_test': False} 
    )
    if not cids:
        return
    contracts_ids = [
        x['polissa'][0]
        for x in O.GiscedataLecturesComptador.read(cids, ['polissa'])
    ]
    contracts_ids = list(set(contracts_ids))
    contracts_ids = O.GiscedataPolissa.search([
        ('id', 'in', contracts_ids),
        ('state', 'not in', ('esborrany', 'validar'))
    ])
    popper = Popper(contracts_ids)
    pops = popper.pop(bucket)
    while pops:
        j = push_contracts.delay(pops)
        logger.info("Job id:%s" % j.id)
        pops = popper.pop(bucket)


def enqueue_contracts(tg_enabled, contracts_id=[]):
    O = setup_peek()
    em = setup_empowering_api()
    # Busquem els que hem d'actualitzar
    search_params = [
        ('state', '=', 'activa'),
        ('etag', '!=', False),
        ('cups.empowering', '=', True)
    ]
    if isinstance(contracts_id, list) and contracts_id:
        search_params.append(('name', 'in', contracts_id))
    polisses_ids = O.GiscedataPolissa.search(search_params)
    if not polisses_ids:
        em.logout()
        return
    for polissa in O.GiscedataPolissa.read(polisses_ids, ['name', 'etag', 'cups','modcontractual_activa']):
        modcons_to_update = []
        is_new_contract = False
        try:
            last_updated = em.contract(polissa['name']).get()['_updated']
            last_updated = make_local_timestamp(last_updated)
        except (libsaas.http.HTTPError, urllib2.HTTPError) as e:
            # A 404 is possible if we delete empowering contracts in insight engine
            # but keep etag in our database.
            # In this case we must force the re-upload as new contract
            if e.code != 404:
                em.logout()
                raise e
            is_new_contract = True
            last_updated = '0'

        building_id = O.EmpoweringCupsBuilding.search([('cups_id', '=', polissa['cups'][0])])

        if building_id:
            w_date = O.EmpoweringCupsBuilding.perm_read(building_id)[0]['write_date']
            if w_date > last_updated:
                modcon_id = polissa['modcontractual_activa'][0]
                modcons_to_update.append(modcon_id)

        if not is_new_contract:
            modcons_id = O.GiscedataPolissaModcontractual.search([('polissa_id','=',polissa['id'])])
            for modcon_id in modcons_id:
                w_date = O.GiscedataPolissaModcontractual.perm_read(modcon_id)[0]['write_date']
                if w_date > last_updated:
                    logger.info('La modcontractual %d a actualitzar write_'
                                'date: %s last_update: %s' % (
                        modcon_id, w_date, last_updated))
                    modcons_to_update.append(modcon_id)
                    continue

                profile_id = O.EmpoweringModcontractualProfile.search([
                            ('modcontractual_id', '=', modcon_id)])


        modcons_to_update = list(set(modcons_to_update))

        if modcons_to_update:
            logger.info('Polissa %s actualitzada a %s després de %s' % (
                polissa['name'], w_date, last_updated))
            push_modcontracts.delay(modcons_to_update, polissa['etag'])
        if is_new_contract:
            logger.info("La polissa %s te etag pero ha estat borrada "
                        "d'empowering, es torna a pujar" % polissa['name'])
            push_contracts.delay([polissa['id']])
    em.logout()

def enqueue_remove_contracts(tg_enabled, contracts_id=[]):
    O = setup_peek()
    em = setup_empowering_api()
    # Busquem els que hem d'actualitzar
    search_params = [
        ('state', '=', 'baixa'),
        ('etag', '!=', False),
        ('cups.empowering', '=', True)
    ]
    if isinstance(contracts_id, list) and contracts_id:
        search_params.append(('name', 'in', contracts_id))
    polisses_ids = O.GiscedataPolissa.search(search_params,
        context={'active_test': False})
    if not polisses_ids:
        em.logout()
        return
    for polissa in O.GiscedataPolissa.read(polisses_ids, ['name', 'etag', 'cups','modcontractual_activa']):
        modcons_to_update = []
        try:
            last_updated = em.contract(polissa['name']).get()['_updated']
            last_updated = make_local_timestamp(last_updated)
        except (libsaas.http.HTTPError, urllib2.HTTPError) as e:
            # A 404 is possible if we delete empowering contracts in insight engine
            # but keep etag in our database.
            raise e

        modcons_id = O.GiscedataPolissaModcontractual.search([('polissa_id','=',polissa['id'])],
            context={'active_test': False})
        for modcon_id in modcons_id:
            w_date = O.GiscedataPolissaModcontractual.perm_read(modcon_id)[0]['write_date']
            if w_date > last_updated:
                logger.info('La modcontractual %d a actualitzar write_'
                            'date: %s last_update: %s' % (
                    modcon_id, w_date, last_updated))
                modcons_to_update.append(modcon_id)
                continue

        modcons_to_update = list(set(modcons_to_update))

        if modcons_to_update:
            logger.info('Polissa %s actualitzada a %s després de %s' % (
                polissa['name'], w_date, last_updated))
            push_modcontracts.delay(modcons_to_update, polissa['etag'])
    em.logout()

@job(setup_queue(name='measures'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_amon_measures(tg_enabled, measures_ids):
    """Pugem les mesures a l'Insight Engine
    """

    em = setup_empowering_api()
    O = setup_peek()
    amon = AmonConverter(O)
    start = datetime.now()
    if tg_enabled:
        mongo = setup_mongodb()
        collection = mongo['tg_billing']
        mdbmeasures = collection.find({'id': {'$in': measures_ids}},
                                      {'name': 1, 'id': 1, '_id': 0,
                                      'ai': 1, 'r1': 1, 'date_end': 1},
                                      sort=[('date_end', pymongo.ASCENDING)])
        measures = [x for x in mdbmeasures]
    else:
        fields_to_read = ['comptador', 'name', 'tipus', 'periode', 'lectura']

        measures = O.GiscedataLecturesLectura.read(measures_ids, fields_to_read)
        # NOTE: Tricky write_date and end_date rename
        for idx, item in enumerate(measures):
            measure_id = item['id']
            measures[idx]['write_date'] = \
                O.GiscedataLecturesLectura.perm_read(measure_id)[0]['write_date']
            measures[idx]['date_end']=measures[idx]['name']

    logger.info("Enviant de %s (id:%s) a %s (id:%s)" % (
        measures[-1]['date_end'], measures[-1]['id'],
        measures[0]['date_end'], measures[0]['id']
    ))
    measures_to_push = amon.measure_to_amon(measures)
    stop = datetime.now()
    logger.info('Mesures transformades en %s' % (stop - start))
    start = datetime.now()

    measures_pushed = em.residential_timeofuse_amon_measures().create(measures_to_push)
    # TODO: Pending to check whether all measure were properly commited

    if tg_enabled:
        for measure in measures:
            from .amon import get_device_serial

            serial = get_device_serial(last_measure['name'])
            cids = O.GiscedataLecturesComptador.search([('name', '=', serial)], context={'active_test': False})
            O.GiscedataLecturesComptador.update_empowering_last_measure(cids, '%s' % measure['write_date'])
        mongo.connection.disconnect()
    else:
        for measure in measures:
          O.GiscedataLecturesComptador.update_empowering_last_measure(
                [measure['comptador'][0]], '%s' % measure['write_date']
          )
    stop = datetime.now()
    logger.info('Mesures enviades en %s' % (stop - start))
    logger.info("%s measures creades" % len(measures_pushed))
    em.logout()


@job(setup_queue(name='contracts'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_modcontracts(modcons, etag):
    """modcons is a list of modcons to push
    """
    em = setup_empowering_api()
    O = setup_peek()
    amon = AmonConverter(O)
    fields_to_read = ['data_inici', 'polissa_id']
    modcons = O.GiscedataPolissaModcontractual.read(modcons, fields_to_read)
    modcons = sorted_by_key(modcons, 'data_inici')
    for modcon in modcons:
        amon_data = amon.contract_to_amon(
            modcon['polissa_id'][0],
            {'modcon_id': modcon['id']}
        )[0]
        response = em.contract(modcon['polissa_id'][1]).update(amon_data, etag)
        if check_response(response, amon_data):
            etag = response['_etag']
    O.GiscedataPolissa.write(modcon['polissa_id'][0], {'etag': etag})
    em.logout()


@job(setup_queue(name='contracts'), connection=setup_redis(), timeout=3600)
@sentry.capture_exceptions
def push_contracts(contracts_id):
    """Pugem els contractes
    """
    em = setup_empowering_api()
    O = setup_peek()
    amon = AmonConverter(O)
    if not isinstance(contracts_id, (list, tuple)):
        contracts_id = [contracts_id]
    for pol in O.GiscedataPolissa.read(contracts_id, ['modcontractuals_ids', 'name']):
        logger.info("Polissa %s" %  pol['name'])
        cid = pol['id']
        upd = []
        first = True
        try:
            for modcon_id in reversed(pol['modcontractuals_ids']):
                amon_data = amon.contract_to_amon(cid,
                        {'modcon_id': modcon_id, 'first':first})[0]
                if first:
                    response = em.contracts().create(amon_data)
                    first = False
                else:
                    etag = upd[-1]['_etag']
                    response = em.contract(pol['name']).update(amon_data, etag)
                if check_response(response, amon_data):
                    upd.append(response)
        except Exception as e:
            logger.info("Exception id: %s %s" % (pol['name'], str(e)))
            continue
        if upd:
            etag = upd[-1]['_etag']
            logger.info("Polissa id: %s -> etag %s" % (pol['name'], etag))
            O.GiscedataPolissa.write(cid, {'etag': etag})
        else:
            logger.info("Polissa id: %s no etag found" % (pol['name']))
    em.logout()
